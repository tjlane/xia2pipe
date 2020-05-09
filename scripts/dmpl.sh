#!/bin/bash

# TJL April/May 2020
# this script is a mess, meant for quick prototyping...
# sorry ;)


# ./dmpl.sh --dir= --metadata= --resolution= --refpdb= --mtzin=

# NOTE: cross referenced from python
#outdir={outdir}
#metadata={metadata}_{run:03d}
#resolution={resolution}
#ref_pdb={reference_pdb}
#input_mtz={input_mtz}


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
    echo "--free_mtz=<path>"        # free_mtz
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


# >> static input (for now)
SCRIPTS_DIR="/home/tjlane/opt/xia2pipe/scripts"


# >> go do the data
cd ${outdir}
echo " --- DIMPLING --- "
echo "chdir: ${outdir}"


# >> uni_free : same origin, reset rfree flags
uni_free=$SCRIPTS_DIR/uni_free.csh
csh ${uni_free} ${input_mtz} ${metadata}_rfree.mtz ${free_mtz}


# >> cut resolution of MTZ
cut_mtz=${metadata}_rfree_rescut.mtz # WORK ON NAME
mtzutils hklin ${metadata}_rfree.mtz \
hklout ${cut_mtz} <<eof
resolution ${resolution}
eof


# >> forcedown uncut reflections & ensure r-free flags propogate
fd=${SCRIPTS_DIR}/force_down
cutdown_mtz=${metadata}_cutdown.mtz

${fd} ${cut_mtz}

sftools <<eof
READ fd-${cut_mtz}
READ ${cut_mtz} COLUMN FreeR_flag
WRITE ${cutdown_mtz}
EXIT
eof


# >> rebuild
phenix.autobuild                            \
  data=${cutdown_mtz}                     \
  model=${ref_pdb}                        \
  nproc=5                                   \
  n_cycle_rebuild_max=5                     \
  multiple_models=True                      \
  multiple_models_number=1                  \
  include_input_model=True                  \
  rebuild_in_place=True                     \
  keep_input_waters=True                    \
  place_waters=No                           \
  s_annealing=False


# >> add riding H
ln -s AutoBuild_run_1_/overall_best.pdb overall_best.pdb
phenix.ready_set overall_best.pdb


# >> phenix reciprocal-space refinement
phenix.refine ${cutdown_mtz} overall_best.updated.pdb                 \
  prefix=${metadata}                                                  \
  serial=2                                                              \
  strategy=individual_sites+individual_adp+individual_sites_real_space  \
  simulated_annealing=True                                              \
  optimize_mask=True                                                    \
  optimize_xyz_weight=True                                              \
  optimize_adp_weight=True                                              \
  simulated_annealing.mode=second_and_before_last                       \
  main.number_of_macro_cycles=7                                         \
  nproc=24                                                              \
  main.max_number_of_iterations=40                                      \
  adp.set_b_iso=20                                                      \
  ordered_solvent={ordered_solvent}                                     \
  simulated_annealing.start_temperature=2500                            
#  refinement.input.xray_data.r_free_flags.file_name=${cut_mtz}        \
#  refinement.input.xray_data.r_free_flags.label=FreeR_flag


# >> real space refinement
phenix.real_space_refine ${metadata}_002.pdb ${cutdown_mtz}


# >> dimple to check for blobs
dimple ${metadata}_002.pdb ${fd_mtz}  \
  --free-r-flags ${cut_mtz}             \
  -f png                                  \
  --jelly 0                               \
  --restr-cycles 0                        \
  --hklout ${metadata}_postphenix_out.mtz \
  --xyzout ${metadata}_postphenix_out.pdb \
  {outdir}
