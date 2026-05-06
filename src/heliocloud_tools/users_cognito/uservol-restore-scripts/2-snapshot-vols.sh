#!/usr/bin/env bash
# =============================================================================
# 2-snapshot-vols.sh
# Create AWS EBS snapshots for every user volume listed in the CSV produced
# by 1-export.sh, then append a "snap_id" column to that same CSV file.
#
# Usage:
#   ./2-snapshot-vols.sh [OPTIONS]
#
# Options:
#   -f, --file      FILE    CSV produced by 1_export_cluster_state.sh
#   -t, --tag       KEY=VAL Extra AWS tag applied to every snapshot (repeatable)
#   --dry-run               Print what would happen without calling AWS
#   --parallel      N       Number of concurrent snapshot requests (default: 5)
#   -h, --help              Show this help message
#
# Output:
#   The input CSV is updated in-place with a new "snap_id" column appended.
#   A backup of the original is saved as <input>.bak before any changes.
# =============================================================================

# ── Defaults ──────────────────────────────────────────────────────────────────
INPUT_CSV=""
DRY_RUN=false
PARALLEL=5
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
EXTRA_TAGS=()

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; NC='\033[0m'; BOLD='\033[1m'

log()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()   { echo -e "${GREEN}[OK]${NC}    $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*" >&2; }
die()  { err "$*"; exit 1; }

# ── Arg parsing ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--file)     INPUT_CSV="$2";       shift 2 ;;
    -t|--tag)      EXTRA_TAGS+=("$2");   shift 2 ;;
    --dry-run)     DRY_RUN=true;         shift ;;
    --parallel)    PARALLEL="$2";        shift 2 ;;
    -h|--help)     sed -n '2,17p' "$0" | sed 's/^# \?//'; exit 0 ;;
    *) die "Unknown option: $1" ;;
  esac
done

[[ -z "${INPUT_CSV}" ]] && die "Supply a CSV with -f/--file"
[[ -f "${INPUT_CSV}" ]] || die "File not found: ${INPUT_CSV}"

command -v aws &>/dev/null || die "'aws' CLI not found."
command -v jq  &>/dev/null || die "'jq' not found."

# Refuse to run if snap_id column already exists — prevents double-runs
# from corrupting the file.
if head -1 "${INPUT_CSV}" | grep -q 'snap_id'; then
  die "Input CSV already contains a 'snap_id' column. Remove it before re-running."
fi

# ── Back up the original CSV before touching it ───────────────────────────────
BACKUP="${INPUT_CSV}.bak"
cp "${INPUT_CSV}" "${BACKUP}"
log "Backup saved: ${BACKUP}"

# ── Temp directory for per-row result files ───────────────────────────────────
# Each background job writes one file: <tmpdir>/<pvc_name>.snap
# This avoids concurrent writes to a shared file from subshells.
TMPDIR_WORK=$(mktemp -d)
trap 'rm -rf "${TMPDIR_WORK}"' EXIT

# ── Tag builder ───────────────────────────────────────────────────────────────
build_tags() {
  local pvc_name="$1" user="$2" vol_id="$3"
  local tags="Key=Vol-Restore-Snapshot,Value=true \
    Key=SourcePVC,Value=${pvc_name} \
    Key=SourceUser,Value=${user} \
    Key=SourceVolume,Value=${vol_id} \
    Key=CreatedAt,Value=${TIMESTAMP} \
    Key=Name,Value=Vol-Restore-Snapshot for user: ${user}"
  for t in "${EXTRA_TAGS[@]+"${EXTRA_TAGS[@]}"}"; do
    tags+=" Key=${t%%=*},Value=${t#*=}"
  done
  echo "$tags"
}

# ── Per-volume snapshot function (runs as a background job) ───────────────────
# Writes a single "<pvc_name>,<snap_id>" line to $TMPDIR_WORK/<pvc_name>.snap
snapshot_volume() {
  local pvc_name="$1" pv_name="$2" ebs_vol="$3" user="$4"
  local result_file="${TMPDIR_WORK}/${pvc_name}.snap"

  # Skip non-EBS entries
  case "${ebs_vol}" in
    EFS_STATIC|UNKNOWN|"")
      warn "Skipping non-EBS entry: ${pvc_name} (${ebs_vol})"
      printf '%s,SKIPPED\n' "${pvc_name}" > "${result_file}"
      return
      ;;
  esac

  local tag_spec
  tag_spec="ResourceType=snapshot,Tags=[$(
    build_tags "${pvc_name}" "${user}" "${ebs_vol}" | \
    sed 's/ Key=/},{Key=/g; s/^/\{/; s/$/\}/'
  )]"

  local description="Vol-Restore-Snapshot for PVC ${pvc_name} / user ${user} / ${TIMESTAMP}"

  if [[ "${DRY_RUN}" == "true" ]]; then
    log "[DRY-RUN] Would snapshot ${ebs_vol} (${pvc_name})"
    printf '%s,DRY-RUN\n' "${pvc_name}" > "${result_file}"
    return
  fi

  local snap_json
  snap_json=$(aws ec2 create-snapshot \
    --volume-id "${ebs_vol}" \
    --description "${description}" \
    --tag-specifications "${tag_spec}" \
    --output json 2>&1) || {
      err "Failed to snapshot ${ebs_vol} for ${pvc_name}: ${snap_json}"
      printf '%s,ERROR\n' "${pvc_name}" > "${result_file}"
      return
    }

  local snapshot_id snap_status
  snapshot_id=$(echo "${snap_json}" | jq -r '.SnapshotId')
  snap_status=$(echo "${snap_json}"  | jq -r '.State')

  ok "Snapshot initiated: ${snapshot_id} for ${ebs_vol} (${pvc_name})"
  printf '%s,%s\n' "${pvc_name}" "${snapshot_id}" > "${result_file}"
}

export -f snapshot_volume build_tags log ok warn err
export TIMESTAMP DRY_RUN TMPDIR_WORK
export EXTRA_TAGS_STR="${EXTRA_TAGS[*]+${EXTRA_TAGS[*]}}"

# ── Phase 1: fire snapshot jobs in parallel ───────────────────────────────────
log "Reading CSV: ${INPUT_CSV}"
log "Parallelism: ${PARALLEL}"
[[ "${DRY_RUN}" == "true" ]] && warn "DRY-RUN mode — no snapshots will be created."

PIDS=()

while IFS=',' read -r pvc_name pv_name status capacity access sc ebs_vol region age_days reclaim vol_mode user_claim; do
  [[ "${pvc_name}" == "pvc_name" ]] && continue  # skip header

  snapshot_volume "${pvc_name}" "${pv_name}" "${ebs_vol}" "${user_claim}" &
  PIDS+=($!)

  if (( ${#PIDS[@]} >= PARALLEL )); then
    wait "${PIDS[0]}"
    PIDS=("${PIDS[@]:1}")
  fi

done < "${INPUT_CSV}"

# Drain remaining jobs
for pid in "${PIDS[@]+"${PIDS[@]}"}"; do wait "$pid"; done
log "All snapshot jobs finished."

# ── Phase 2: wait for snapshots to reach 'completed' ─────────────────────────
if [[ "${DRY_RUN}" == "false" ]]; then
  log "Waiting for snapshots to complete (this may take several minutes)..."

  SNAP_IDS=$(grep -h '' "${TMPDIR_WORK}"/*.snap 2>/dev/null \
    | awk -F',' '$2 ~ /^snap-/ {printf "%s ", $2}')

  if [[ -n "${SNAP_IDS}" ]]; then
    DEADLINE=$(( $(date +%s) + 1800 ))
    while true; do
      # shellcheck disable=SC2086
      PENDING=$(aws ec2 describe-snapshots \
        --snapshot-ids ${SNAP_IDS} \
        --query 'Snapshots[?State!=`completed`].SnapshotId' \
        --output text 2>/dev/null | wc -w)

      if (( PENDING == 0 )); then
        ok "All snapshots completed!"
        break
      fi

      if (( $(date +%s) > DEADLINE )); then
        warn "Timed out waiting for ${PENDING} snapshot(s). Check the AWS console."
        break
      fi

      log "${PENDING} snapshot(s) still pending... (checking again in 30s)"
      sleep 30
    done
  fi
fi

# ── Phase 3: rewrite the CSV with snap_id appended ────────────────────────────
# Build a lookup map from the per-job result files: pvc_name -> snap_id
declare -A SNAP_MAP
for result_file in "${TMPDIR_WORK}"/*.snap; do
  [[ -f "${result_file}" ]] || continue
  pvc=$(cut -d',' -f1 "${result_file}")
  snap=$(cut -d',' -f2 "${result_file}")
  SNAP_MAP["${pvc}"]="${snap}"
done

ENRICHED_CSV=$(mktemp)

# Write new header
head -1 "${INPUT_CSV}" | tr -d '\n' >> "${ENRICHED_CSV}"
printf ',snap_id\n' >> "${ENRICHED_CSV}"

# Write each data row with snap_id appended, preserving original column order
tail -n +2 "${INPUT_CSV}" | while IFS=',' read -r pvc_name rest; do
  snap_id="${SNAP_MAP[${pvc_name}]:-MISSING}"
  printf '%s,%s,%s\n' "${pvc_name}" "${rest}" "${snap_id}" >> "${ENRICHED_CSV}"
done

# Atomically replace the input CSV
mv "${ENRICHED_CSV}" "${INPUT_CSV}"
ok "CSV updated in-place: ${INPUT_CSV}"

# ── Summary ───────────────────────────────────────────────────────────────────
TOTAL=$(( $(wc -l < "${INPUT_CSV}") - 1 ))
SNAPPED=$(awk -F',' 'NR>1 && $NF ~ /^snap-/' "${INPUT_CSV}" | wc -l)
SKIPPED=$(awk -F',' 'NR>1 && $NF=="SKIPPED"' "${INPUT_CSV}" | wc -l)
ERRORS=$( awk -F',' 'NR>1 && ($NF=="ERROR" || $NF=="MISSING")' "${INPUT_CSV}" | wc -l)

echo ""
echo -e "${BOLD}═══════════════════════════════════════${NC}"
echo -e "${BOLD} Snapshot Summary${NC}"
echo -e "${BOLD}═══════════════════════════════════════${NC}"
echo -e "  Total rows      : ${TOTAL}"
echo -e "  Snapshots taken : ${GREEN}${SNAPPED}${NC}"
echo -e "  Skipped (EFS)   : ${YELLOW}${SKIPPED}${NC}"
echo -e "  Errors/Missing  : ${RED}${ERRORS}${NC}"
echo -e "  Output CSV      : ${BOLD}${INPUT_CSV}${NC}"
echo -e "  Original backup : ${BOLD}${BACKUP}${NC}"
echo -e "${BOLD}═══════════════════════════════════════${NC}"
