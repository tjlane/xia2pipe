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


# >> DEFAULTS
ordered_solvent=False
SCRIPTS_DIR="/home/tjlane/opt/xia2pipe/scripts"


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
    echo "--no-ordered-sol"         # ordered_solvent
    echo "--scriptdir=<path>"       # SCRIPTS_DIR"
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
        --no-ordered-sol)
            ordered_solvent=True
            ;;
        --scriptdir)
            SCRIPTS_DIR=$VALUE
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



# >> go do the data
cd ${outdir}
echo "chdir: ${outdir}"


# >> uni_free : same origin, reset rfree flags
uni_free=$SCRIPTS_DIR/uni_free.csh
csh ${uni_free} ${input_mtz} ${metadata}_rfree.mtz ${free_mtz}


# >> cut resolution of MTZ
cut_mtz=${metadata}_rfree_rescut.mtz
mtzutils hklin ${metadata}_rfree.mtz \
hklout ${cut_mtz} <<eof
resolution ${resolution}
eof


# >> forcedown uncut reflections & ensure r-free flags propogate
fd=${SCRIPTS_DIR}/force_down
cutdown_mtz=${metadata}_cutdown.mtz
${fd} ${cut_mtz}


# >> copy r-free flags to cutdown mtz
sftools <<eof
READ fd-${cut_mtz}
READ ${cut_mtz} COLUMN FreeR_flag
WRITE ${cutdown_mtz}
EXIT
eof


# >> cleanup
rm ${metadata}_rfree_rescut.mtz
rm ${cut_mtz}
rm fd-${cut_mtz}


# >> id input model
ln -sf ${ref_pdb} pre_refined.pdb


# >> add riding H
phenix.ready_set pre_refined.pdb


# >> phenix reciprocal-space refinement
phenix.refine --overwrite                                               \
  ${cutdown_mtz}                                                        \
  pre_refined.updated.pdb                                               \
  prefix=${metadata}                                                    \
  serial=1                                                              \
  strategy=individual_sites+individual_adp+individual_sites_real_space+rigid_body \
  simulated_annealing=True                                              \
  optimize_mask=True                                                    \
  optimize_xyz_weight=True                                              \
  optimize_adp_weight=True                                              \
  simulated_annealing.mode=first_half                                   \
  main.number_of_macro_cycles=6                                         \
  nproc=4                                                               \
  main.max_number_of_iterations=40                                      \
  adp.set_b_iso=20                                                      \
  ordered_solvent=${ordered_solvent}                                    \
  simulated_annealing.start_temperature=5000                            \
  allow_polymer_cross_special_position=True

# >> real space refinement
phenix.real_space_refine \
  ${metadata}_001.pdb    \
  ${metadata}_001.mtz    \
  label="2FOFCWT,PH2FOFCWT" \
  allow_polymer_cross_special_position=True


# >> rounds of reciprocal/real refinement

NEXT_INPUT_PDB=${metadata}_001_real_space_refined.pdb

for SERIAL in {2..2}
do
    SERIAL_FMT=`printf "%03d" ${SERIAL}`

    phenix.refine --overwrite                                               \
      ${cutdown_mtz}                                                        \
      ${NEXT_INPUT_PDB}                                                     \
      prefix=${metadata}                                                    \
      serial=$SERIAL                                                        \
      strategy=individual_sites+individual_adp+individual_sites_real_space  \
      simulated_annealing=False                                             \
      optimize_mask=True                                                    \
      optimize_xyz_weight=True                                              \
      optimize_adp_weight=True                                              \
      main.number_of_macro_cycles=5                                         \
      nproc=4                                                               \
      main.max_number_of_iterations=40                                      \
      ordered_solvent=${ordered_solvent}                                    \

    phenix.real_space_refine           \
      ${metadata}_${SERIAL_FMT}.pdb    \
      ${metadata}_${SERIAL_FMT}.mtz    \
      label="2FOFCWT,PH2FOFCWT"

    NEXT_INPUT_PDB=${metadata}_${SERIAL_FMT}_real_space_refined.pdb

done


# >> dimple to check for blobs
dimple                                    \
  ${metadata}_${SERIAL_FMT}.pdb           \
  ${cutdown_mtz}                          \
  -M1                                     \
  --free-r-flags ${cutdown_mtz}           \
  -f png                                  \
  --jelly 20                              \
  --restr-cycles 5                        \
  --hklout ${metadata}_postphenix_out.mtz \
  --xyzout ${metadata}_postphenix_out.pdb \
  ./dimple


