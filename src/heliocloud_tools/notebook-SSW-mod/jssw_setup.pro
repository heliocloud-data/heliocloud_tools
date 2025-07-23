; HELIOCLOUD SSW: Global location of SSW and sets path to SSW plus home directory
; usage: ".run jssw_setup"
;setenv,'SSW=/efs/solarsoft'
; this one toggles the plot/oplot/tv/tvscl/xyout replacements
setenv,'SSWJUPYTER=1'
; this is often required; IDL Jupyter has a problem with legacy paths.
;file_delete,'~/.idl/idl/pref-10-idl_8_8-unix/idl.pref',/VERBOSE

; defining common block for the Jupyter Notebook helper routines
COMMON JNSSW, jn_plot, jn_tv

; You can add items to this path if you wish. Format is ":+NEW_PATH_TO_ADD"
pref_set,'IDL_PATH',getenv('SSW')+':+<IDL_DEFAULT>:+'+getenv('HOME'),/COMMIT

; add SSWDB, default is HOME/sswdb
if getenv('SSWDB') EQ '' then setenv,'SSWDB='+getenv('HOME')+'/sswdb'

; USER CHOICE: as user, need to set these choices an appropriate set of values
;setenv,'SSW_INSTR=gen stereo secchi psp festival s3drs lasco'
;setenv,'SSWDB=/efs/skantunes/ssw-mini/sswdb'
;if getenv('SSW_MISSIONS') EQ '' then setenv,'SSW_MISSIONS=soho yohkoh spartan trace cgro smm packages hessi radio optical hxrs smei goes goesr goesn hinode vobs stereo proba2 iris hic so psp'
;setenv,'SSW_MISSION=soho packages stereo psp'

; MIGHT need some additions here for other missions
setenv,'SSW_SOHO_INSTR=soho/cds soho/eit soho/sumer soho/lasco soho/mdi soho/uvcs'
setenv,'SSW_STEREO_INSTR=stereo/impact stereo/plastic stereo/secchi stereo/swaves stereo/ssc'
setenv,'SSW_PSP_INSTR=psp/fields psp/isis psp/wispr psp/sweap'


; EVERYTHING BELOW THIS LINE IS PROBABLY GOOD

; SSW ANTICIPATED TERMINAL PROBLEMS, THIS MAKES JUPTYER WORK BETTER
setenv,'ssw_nomore=1'
;more = 1-keyword_set(nomore) and getenv('ys_nomore') eq '' and getenv('ssw_nomore') eq ''       
;print,more

setenv,'env_list='+getenv('SSW')+'/site/setup/setup.ssw_paths'

setenv,'SSW_PACKAGES_INSTR=packages/binaries packages/mjastereo packages/sbrowser packages/nrl packages/sunspice'
setenv,'SSW_INSTR_ALL=gen soho/cds soho/eit soho/sumer soho/lasco soho/mdi soho/uvcs yohkoh/bcs yohkoh/hxt yohkoh/sxt yohkoh/wbs spartan/spartan trace/trace smm/xrp smm/hxrbs smm/uvsp smm/cp smm/grs smm/cp smm/hxis smm/acrim cgro/batse hessi/hessi packages/binaries packages/chianti packages/spex packages/xray packages/goes packages/cdscat packages/ztools packages/lparl packages/ana packages/mjastereo packages/findstuff packages/vdem packages/andril packages/hydro packages/pfss packages/pdl packages/poa packages/ngdc packages/cmes packages/nlfff packages/cactus packages/spvm packages/sbrowser packages/tplot packages/festival packages/panorama packages/nrl packages/corimp packages/s3drs packages/mkit packages/helioviewer packages/azam packages/gx_simulator packages/swpc_cat packages/euvdeconpak packages/forward packages/desat packages/sunspice packages/mosic packages/cruiser packages/gsfit packages/dem_sites packages/catch packages/simple_reg_dem packages/demreg radio/ethz radio/fhnw radio/nrh radio/norh radio/eovsa radio/ovsa radio/ovro radio/norp radio/mwa radio/lofar optical/soon optical/lapalma optical/nso optical/mees hxrs/hxrs smei/smei goes/sxig12 goes/sxig13 goesr/suvi goesn/sxi hinode/eis hinode/xrt hinode/sot vobs/cosec vobs/egso vobs/vso vobs/ontology vobs/helio stereo/impact stereo/plastic stereo/secchi stereo/swaves stereo/ssc proba2/swap proba2/lyra sdo/aia sdo/hmi sdo/eve iris/iris hic/hic so/stix so/spice psp/fields psp/isis psp/wispr psp/sweap'

setenv,'SSW_PACKAGES='+getenv('SSW')+'/packages'
setenv,'SSW_BINARIES='+getenv('SSW')+'/packages/binaries'
setenv,'SSW_MJASTEREO='+getenv('SSW')+'/packages/mjastereo'
setenv,'SSW_SBROWSER='+getenv('SSW')+'/packages/sbrowser'
setenv,'SSW_NRL='+getenv('SSW')+'/packages/nrl'
setenv,'SSW_SUNSPICE='+getenv('SSW')+'/packages/sunspice'

setenv,'SSW_PACKAGES_ALL='+getenv('SSW')+'/packages/binaries '+getenv('SSW')+'/packages/mjastereo '+getenv('SSW')+'/packages/nrl '+getenv('SSW')+'/packages/sbrowser '+getenv('SSW')+'/packages/sunspice'

setenv,'SSW_SETUP='+getenv('SSW')+'/gen/setup'
setenv,'SSW_GEN_SETUP='+getenv('SSW')+'/gen/setup'
setenv,'SSW_SETUP_DATA='+getenv('SSW')+'/gen/setup/data'
setenv,'SSW_BIN='+getenv('SSW')+'/gen/bin'
setenv,'SSW_GEN_DATA='+getenv('SSW')+'/gen/data'
setenv,'DIR_GEN_SPECTRA='+getenv('SSW')+'/gen/data/spectra'
setenv,'SSW_SITE_LOGS='+getenv('SSW')+'/site/logs'
setenv,'SSW_SITE_SETUP='+getenv('SSW')+'/site/setup'
setenv,'SSW_SITE_MIRROR='+getenv('SSW')+'/site/mirror'
setenv,'TIME_CONV='+getenv('SSW')+'/gen/data/time'
setenv,'SSW_LIBRARIES='+getenv('SSW')+'/gen/idl_libs'
setenv,'IDL_SSWASTRON='+getenv('SSW')+'/gen/idl_libs/astron'
setenv,'SSW_PERL='+getenv('SSW')+'/gen/perl'
setenv,'SSW_URL_GET='+getenv('SSW')+'/gen/perl/url_get'
setenv,'ssw_contrib_master=sohoftp.nascom.nasa.gov'
setenv,'SSW_HELP='+getenv('SSW')+'/gen/idl/help'
setenv,'ZDBASE_SOHO='+getenv('SSW')+'/soho/gen/data/plan/database'
setenv,'ZDBASE='+getenv('SSW')+'/soho/gen/data/plan/database'
setenv,'SOHO_CAP='+getenv('SSW')+'/soho/gen/data/plan/soho_cap'
setenv,'SOHO_SPICE_GEN='+getenv('SSW')+'/soho/gen/data/spice'

setenv,'STEREO_SPICE='+getenv('SSW')+'/stereo/gen/data/spice'
setenv,'STEREO_SPICE_GEN='+getenv('SSW')+'/stereo/gen/data/spice/gen'
setenv,'STEREO_SPICE_SCLK='+getenv('SSW')+'/stereo/gen/data/spice/sclk'
setenv,'STEREO_SPICE_ATTIT_SM='+getenv('SSW')+'/stereo/gen/data/spice/ah'
setenv,'STEREO_SPICE_EPHEM='+getenv('SSW')+'/stereo/gen/data/spice/epm'
setenv,'STEREO_SPICE_DEF_EPHEM='+getenv('SSW')+'/stereo/gen/data/spice/depm'
setenv,'STEREO_SPICE_OTHER='+getenv('SSW')+'/stereo/gen/data/spice/other'
setenv,'STEREO_SPICE_ATTITUDE=./stereo/gen/spice/ah'
setenv,'SOLO_SUNSPICE='+getenv('SSW')+'/so/gen/data/sunspice'
setenv,'PSP_SPICE='+getenv('SSW')+'/psp/gen/data/spice'

setenv,'SSW_GEN='+getenv('SSW')+'/gen'
setenv,'SSW_CDS='+getenv('SSW')+'/soho/cds'
setenv,'SSW_EIT='+getenv('SSW')+'/soho/eit'
setenv,'SSW_SUMER='+getenv('SSW')+'/soho/sumer'
setenv,'SSW_LASCO='+getenv('SSW')+'/soho/lasco'
setenv,'SSW_MDI='+getenv('SSW')+'/soho/mdi'
setenv,'SSW_UVCS='+getenv('SSW')+'/soho/uvcs'
setenv,'SSW_IMPACT='+getenv('SSW')+'/stereo/impact'
setenv,'SSW_PLASTIC='+getenv('SSW')+'/stereo/plastic'
setenv,'SSW_SECCHI='+getenv('SSW')+'/stereo/secchi'
setenv,'SSW_SWAVES='+getenv('SSW')+'/stereo/swaves'
setenv,'SSW_SSC='+getenv('SSW')+'/stereo/ssc'
setenv,'SSW_ISIS='+getenv('SSW')+'/psp/isis'
setenv,'SSW_WISPR='+getenv('SSW')+'/psp/wispr'
setenv,'SSW_SWEAP='+getenv('SSW')+'/psp/sweap'
setenv,'SCC_DATA='+getenv('SSW')+'/stereo/secchi/data'
setenv,'PT='+getenv('SSW')+'/stereo/secchi/data/PT'
setenv,'SECCHI_CAL='+getenv('SSW')+'/stereo/secchi/calibration'
setenv,'SECCHI_LIB='+getenv('SSW')+'/stereo/secchi/lib'
setenv,'EUVI_RESPONSE='+getenv('SSW')+'/stereo/secchi/calibration/euvi_response'
setenv,'LASCO_EPHEMERIS='+getenv('SSW')+'/soho/lasco/idl/astrometry/ephemeris'
setenv,'LASCO_EXTERNAL='+getenv('SSW')+'/soho/lasco/idl/astrometry/external'
setenv,'LASCO_CATALOGS='+getenv('SSW')+'/soho/lasco/idl/astrometry/catalogs'
setenv,'HOMED='+getenv('SSW')+'/soho/lasco/idl/display'
setenv,'LASCO_DATA='+getenv('SSW')+'/soho/lasco/idl/data'
setenv,'EIT_DEGRID='+getenv('SSW')+'/soho/lasco/idl/nrleit/data/eit_degrid'

; from Icy docs
dlm_register, getenv('SSW')+'/packages/sunspice/icy/linux_x86_64/lib/icy.dlm
pref_set,'IDL_DLM_PATH',getenv('SSW')+'/packages/sunspice/icy/linux_x86_64/lib:<IDL_DEFAULT>',/COMMIT
; next 2 lines are just to validate if, if you need to debug you can comment them out
;print, cspice_tkvrsn( 'TOOLKIT' )
;help, 'icy', /dlm
; from packages/sunspice/setup
setup_sunspice
register_sunspice_dlm
setenv,'SSW_SUNSPICE_GEN='+getenv('SSW')+'/packages/sunspice/data'

END
