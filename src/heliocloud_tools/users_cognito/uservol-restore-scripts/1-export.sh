#!/usr/bin/env bash
# =============================================================================
# 1-export.sh
# Query the current cluster's PVC/PV layout and export a migration CSV.
#
# Usage:
#   ./1-export.sh [OPTIONS]
#
# Options:
#   -n, --namespace  NAMESPACE   Kubernetes namespace (default: daskhub)
#   -o, --output     FILE        Output CSV file (default: user-vols-<timestamp>.csv)
#   -k, --kubeconfig FILE        Path to kubeconfig (optional)
#   -h, --help                   Show this help message
# =============================================================================

# ── Defaults ──────────────────────────────────────────────────────────────────
NAMESPACE="daskhub"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_FILE="user-vols-${TIMESTAMP}.csv"
KUBECONFIG_ARG=""

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; NC='\033[0m'; BOLD='\033[1m'

log()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()   { echo -e "${GREEN}[OK]${NC}    $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ── Arg parsing ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    -n|--namespace)  NAMESPACE="$2";      shift 2 ;;
    -o|--output)     OUTPUT_FILE="$2";    shift 2 ;;
    -k|--kubeconfig) KUBECONFIG_ARG="--kubeconfig $2"; shift 2 ;;
    -h|--help)
      sed -n '2,14p' "$0" | sed 's/^# \?//'
      exit 0 ;;
    *) err "Unknown option: $1"; exit 1 ;;
  esac
done

KUBECTL="kubectl ${KUBECONFIG_ARG} -n ${NAMESPACE}"

# ── Pre-flight checks ─────────────────────────────────────────────────────────
for cmd in kubectl aws jq; do
  command -v "$cmd" &>/dev/null || { err "'$cmd' is not installed or not in PATH."; exit 1; }
done

log "Querying namespace '${NAMESPACE}'..."

# ── Fetch PVCs ────────────────────────────────────────────────────────────────
PVC_JSON=$(${KUBECTL} get pvc -o json 2>/dev/null) || {
  err "Failed to list PVCs. Check your kubeconfig / namespace."
  exit 1
}

PVC_COUNT=$(echo "$PVC_JSON" | jq '.items | length')
log "Found ${PVC_COUNT} PVCs."

# ── Write CSV header ──────────────────────────────────────────────────────────
echo "pvc_name,pv_name,status,capacity,access_modes,storage_class,ebs_volume_id,aws_region,age_days,reclaim_policy,volume_mode,user_claim" \
  > "${OUTPUT_FILE}"

# ── Process each PVC ─────────────────────────────────────────────────────────
EXPORTED=0

while IFS= read -r item; do
  PVC_NAME=$(echo "$item"    | jq -r '.metadata.name')
  STATUS=$(echo "$item"      | jq -r '.status.phase')
  CAPACITY=$(echo "$item"    | jq -r '.status.capacity.storage // .spec.resources.requests.storage')
  ACCESS=$(echo "$item"      | jq -r '[.spec.accessModes[]?] | join("|")')
  SC=$(echo "$item"          | jq -r '.spec.storageClassName // ""')
  PV_NAME=$(echo "$item"     | jq -r '.spec.volumeName // ""')
  VOLUME_MODE=$(echo "$item" | jq -r '.spec.volumeMode // "Filesystem"')

  # Age in days
  CREATION=$(echo "$item" | jq -r '.metadata.creationTimestamp')
  AGE_DAYS=$(( ( $(date +%s) - $(date -d "${CREATION}" +%s 2>/dev/null || date -jf "%Y-%m-%dT%H:%M:%SZ" "${CREATION}" +%s 2>/dev/null || echo 0) ) / 86400 ))

  # Derive a "user" name from the claim name (strip leading "claim-")
  USER_CLAIM=$(echo "$PVC_NAME" | sed 's/^claim-//')

  # ── Look up the EBS Volume ID from the PV ──────────────────────────────────
  EBS_VOLUME_ID=""
  AWS_REGION=""
  RECLAIM_POLICY=""

  if [[ -n "${PV_NAME}" ]]; then
    PV_JSON=$(kubectl ${KUBECONFIG_ARG} get pv "${PV_NAME}" -o json 2>/dev/null) || true

    if [[ -n "${PV_JSON}" ]]; then
      RECLAIM_POLICY=$(echo "$PV_JSON" | jq -r '.spec.persistentVolumeReclaimPolicy // "Delete"')

      # Try CSI source first (newer clusters)
      EBS_VOLUME_ID=$(echo "$PV_JSON" | jq -r '.spec.csi.volumeHandle // ""')

      # Fall back to in-tree awsElasticBlockStore
      if [[ -z "${EBS_VOLUME_ID}" ]]; then
        EBS_VOLUME_ID=$(echo "$PV_JSON" | jq -r '.spec.awsElasticBlockStore.volumeID // ""')
        # Strip "aws://us-east-1a/vol-..." prefix if present
        EBS_VOLUME_ID=$(echo "$EBS_VOLUME_ID" | grep -oP 'vol-[a-f0-9]+' || echo "")
      fi

      # Query AWS for the region/AZ tag on the volume
      if [[ -n "${EBS_VOLUME_ID}" ]]; then
        AWS_REGION=$(aws ec2 describe-volumes \
          --volume-ids "${EBS_VOLUME_ID}" \
          --query 'Volumes[0].AvailabilityZone' \
          --output text 2>/dev/null | sed 's/[a-z]$//' || echo "unknown")
      fi
    fi
  fi

  # ── Handle non-EBS / EFS static volumes ───────────────────────────────────
  if [[ -z "${EBS_VOLUME_ID}" ]]; then
    if [[ "${PV_NAME}" == "efs-"* ]]; then
      EBS_VOLUME_ID="EFS_STATIC"
    else
      EBS_VOLUME_ID="UNKNOWN"
      warn "Could not resolve EBS volume ID for PVC '${PVC_NAME}' / PV '${PV_NAME}'"
    fi
  fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "${PVC_NAME}" "${PV_NAME}" "${STATUS}" "${CAPACITY}" \
    "${ACCESS}" "${SC}" "${EBS_VOLUME_ID}" "${AWS_REGION}" \
    "${AGE_DAYS}" "${RECLAIM_POLICY}" "${VOLUME_MODE}" "${USER_CLAIM}" \
    >> "${OUTPUT_FILE}"

  ok "Exported: ${PVC_NAME} → ${EBS_VOLUME_ID}"
  (( EXPORTED++ ))

done < <(echo "$PVC_JSON" | jq -c '.items[]')

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}═══════════════════════════════════════${NC}"
echo -e "${BOLD} Export Summary${NC}"
echo -e "${BOLD}═══════════════════════════════════════${NC}"
echo -e "  Namespace : ${NAMESPACE}"
echo -e "  Exported  : ${GREEN}${EXPORTED}${NC}"
echo -e "  Output    : ${BOLD}${OUTPUT_FILE}${NC}"
echo -e "${BOLD}═══════════════════════════════════════${NC}"
