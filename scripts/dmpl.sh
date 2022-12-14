#!/bin/bash

# TJL April/May 2020
# this script is a mess, meant for quick prototyping...
# sorry ;)

# ./dmpl.sh --dir= --metadata= --resolution= --refpdb= --mtzin= ...


# >> DEFAULTS
ordered_solvent=True
SCRIPTS_DIR="/home/tjlane/opt/xia2pipe/scripts"
NPROC=1
forcedown=false


# >> parse arguments
function usage()
{
    echo "run the dimpling automatic refinement pipeline"
    echo ""
    echo "./dmpl.sh"
    echo "-h --help"                # ^ below are bash variable names
    echo "--dir=<path>"             # outdir
    echo "--metadata=<md_run-id>"   # metadata
    echo "--resolution=<float>"     # resolution
    echo "--refpdb=<path>"          # ref_pdb
    echo "--mtzin=<path>"           # input_mtz
    echo "--freemtz=<path>"         # free_mtz
    echo "--dont-place-waters"      # ordered_solvent
    echo "--scriptdir=<path>"       # SCRIPTS_DIR"
    echo "--nproc=<int>"            # NPROC
    echo "--forcedown"              # apply forcedown apodization
}

while [ "$1" != "" ]; do
    PARAM=`echo $1 | awk -F= '{print $1}'`
    VALUE=`echo $1 | awk -F= '{print $2}'`
    case $PARAM in
        -h | --help)
            usage
            exit
            ;;
        --dir)
            outdir=$VALUE
            ;;
        --metadata)
            metadata=$VALUE
            ;;
        --resolution)
            resolution=$VALUE
            ;;
        --refpdb)
            ref_pdb=$VALUE
            ;;
        --mtzin)
            input_mtz=$VALUE
            ;;
        --freemtz)
            free_mtz=$VALUE
            ;;
        --dont-place-waters)
            ordered_solvent=False
            ;;
        --scriptdir)
            SCRIPTS_DIR=$VALUE
            ;;
        --nproc)
            NPROC=$VALUE
            ;;
        --forcedown)
            forcedown=true
            ;;
        *)
            echo "ERROR: unknown parameter \"$PARAM\""
            usage
            exit 1
            ;;
    esac
    shift
done

echo " ==== DIMPLING ==== "
echo ""
echo "outdir=     ${outdir}"
echo "metadata=   ${metadata}"
echo "resolution= ${resolution}"
echo "ref_pdb=    ${ref_pdb}"
echo "input_mtz=  ${input_mtz}"
echo "free_mtz=   ${free_mtz}"
echo "ordersol=   ${ordered_solvent}"
echo "scriptdir=  ${SCRIPTS_DIR}"
echo "nproc=      ${NPROC}"
echo "forcedown=  ${forcedown}"



# >> go do the data
cd ${outdir}
echo "chdir: ${outdir}"


# >> if the mtz is from staraniso, drop SA_flag
#    http://staraniso.globalphasing.org/test_set_flags_about.html 
#    if the mtz doesn't have the SA_flag col, this does nothing
sftools <<eof
READ ${input_mtz}
SELECT COL SA_flag NOT absent
WRITE ${input_mtz}
Y
EXIT 
eof


# >> uni_free : same origin, set rfree flags to the common set
uni_free=$SCRIPTS_DIR/uni_free.csh
csh ${uni_free} ${input_mtz} ${metadata}_rfree.mtz ${free_mtz}


# >> cut resolution of MTZ
cut_mtz=${metadata}_cut.mtz
mtzutils hklin ${metadata}_rfree.mtz \
hklout ${cut_mtz} <<eof
resolution ${resolution}
eof

#rm ${metadata}_rfree.mtz


# >> forcedown uncut reflections & ensure r-free flags propogate
if ${forcedown}; then

  echo ""
  echo "running forcedown"
  cutdown_mtz=${metadata}_cutdown.mtz

  fd=${SCRIPTS_DIR}/force_down
  ${fd} ${cut_mtz}

  sftools <<eof
READ fd-${cut_mtz}
DELETE COLUMN SIGFP
READ ${cut_mtz} COLUMN FreeR_flag
READ ${cut_mtz} COLUMN SIGF
SET LABELS COL SIGF
SIGFP
WRITE ${cutdown_mtz} COLUMN FreeR_flag FP SIGFP
EXIT
eof

# 9 Jan 2021 : seems like forcedown is not copying over the 
#              sigmas correctly...
#READ fd-${cut_mtz}
#READ ${cut_mtz} COLUMN FreeR_flag
#WRITE ${cutdown_mtz}
#EXIT
#eof

  #rm fd-${cut_mtz}
  # >> end up with _cutdown.mtz

else
  cutdown_mtz=${cut_mtz}
  # >> end up with _cut.mtz

fi


# >> id input model
#ln -sf ${ref_pdb} pre_refined.pdb
ref_pdb_list=`echo ${ref_pdb} | tr ',' ' '`


# >> dimple round 1

# TJL 17DEC2020
# I re-enabled the default setting for MR as the latest
# crystals have highly variable unit cells
#  -M1                                     \


dimple                                    \
  --free-r-flags ${cutdown_mtz}           \
  --jelly 0                               \
  --restr-cycles 0                        \
  --hklout ${metadata}_dimple-MR.mtz      \
  --xyzout ${metadata}_dimple-MR.pdb      \
  ${cutdown_mtz}                          \
  ${ref_pdb_list}                         \
  .


# >> add riding H
phenix.ready_set ${metadata}_dimple-MR.pdb


# >> rigid body refinement, adp refinement --> get basic location correct
#    (serial 1: *_001.pdb)
phenix.refine --overwrite                                               \
  ${cutdown_mtz}                                                        \
  ${metadata}_dimple-MR.updated.pdb                                     \
  prefix=${metadata}                                                    \
  serial=1                                                              \
  strategy=rigid_body+individual_adp                                    \
  optimize_mask=True                                                    \
  main.number_of_macro_cycles=5                                         \
  main.max_number_of_iterations=60                                      \
  rigid_body.mode=every_macro_cycle                                     \
  adp.set_b_iso=20                                                      \
  refinement.input.symmetry_safety_check=warning

# >> SA --> allow structure to escape local minimum 
#    (serial 2: *_002.pdb)
phenix.refine --overwrite                                               \
  ${cutdown_mtz}                                                        \
  ${metadata}_001.pdb                                                   \
  prefix=${metadata}                                                    \
  serial=2                                                              \
  strategy=individual_sites+individual_adp+individual_sites_real_space  \
  simulated_annealing=True                                              \
  simulated_annealing_torsion=False                                     \
  optimize_mask=True                                                    \
  optimize_xyz_weight=True                                              \
  optimize_adp_weight=True                                              \
  simulated_annealing.mode=every_macro_cycle                            \
  main.number_of_macro_cycles=1                                         \
  nproc=${NPROC}                                                        \
  main.max_number_of_iterations=60                                      \
  ordered_solvent=${ordered_solvent}                                    \
  simulated_annealing.start_temperature=2500                            \
  allow_polymer_cross_special_position=True


# >> real space refine --> get structure into reasonable geometry
#    (*_002_real_space_refine.pdb)
phenix.real_space_refine    \
  ${metadata}_002.pdb       \
  ${metadata}_002.mtz       \
  label='2FOFCWT,PH2FOFCWT' \
  nproc=${NPROC}            \
  allow_polymer_cross_special_position=True


# >> final refinement, fairly standard --> relax into local minimum
#    (serial 3: *_003.pdb)
phenix.refine --overwrite                                                   \
  ${cutdown_mtz}                                                            \
  ${metadata}_002_real_space_refined.pdb                                    \
  prefix=${metadata}                                                        \
  serial=3                                                                  \
  strategy=individual_sites+individual_adp+individual_sites_real_space+tls  \
  tls.one_residue_one_group=True                                            \
  tls.find_automatically=False                                              \
  simulated_annealing=False                                                 \
  optimize_mask=True                                                        \
  optimize_xyz_weight=True                                                  \
  optimize_adp_weight=True                                                  \
  main.number_of_macro_cycles=8                                             \
  nproc=${NPROC}                                                            \
  main.max_number_of_iterations=60                                          \
  ordered_solvent=${ordered_solvent}                                        \
  allow_polymer_cross_special_position=True


# >> dimple to check for blobs
dimple                                    \
  -M1                                     \
  --free-r-flags ${cutdown_mtz}           \
  -f png                                  \
  --jelly 0                               \
  --restr-cycles 0                        \
  --hklout ${metadata}_postphenix_out.mtz \
  --xyzout ${metadata}_postphenix_out.pdb \
  ${cutdown_mtz}                          \
  ${metadata}_003.pdb                     \
  ./dimple


