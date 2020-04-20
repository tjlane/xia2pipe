
import os
import re
import time
import subprocess
import xml.etree.ElementTree as ET
from os.path import join as pjoin

from projbase import ProjectBase


class DimplingDaemon(ProjectBase):

    def __init__(self, name, pipeline, projpath,
                 spacegroup=None, unit_cell=None):

        super().__init__(name, pipeline, projpath,
                         spacegroup=None, unit_cell=None)

        return


    def fetch_xia_successes(self):

        # TODO later on make better use of the DB...
        successes = self.db.fetch(
            "SELECT metadata FROM Master_View WHERE diffraction='Success';"
        )

        to_run = []
        for md in [ s['metadata'] for s in successes ]:
            if self.xia_result(md) == 'finished':
                to_run.append(md)

        return to_run


    def fetch_running_jobs(self):
        """
        Return a list of metadata str that are running on SLURM
        """

        running = []

        r = subprocess.run('sacct --format="JobID,JobName%30"',
                           capture_output=True, shell=True, check=True)

        lines = r.stdout.decode("utf-8").split('\n')
        for line in lines:
            g = re.search('{}-dmpl_(\w+)'.format(self.name), line)
            if g:
                running.append(g.groups()[0])

        return running


    def get_resolution(self, metadata):
        return self._get_xds_res(metadata)


    def _get_xds_res(self, metadata, which='cc'):

        if which not in ['cc', 'isigma']:
            raise ValueError("which must be `cc` or `isigma`")

        crystal_id = self.metadata_to_id(metadata)

        res = self.db.fetch(
            "SELECT resolution_{} FROM XDS_Data_Reduction WHERE "
            "crystal_id='{}';".format(which, crystal_id)
        )

        assert len(res) == 1, res

        return res[0]['resolution_{}'.format(which)]


    def _get_aimless_res(self, metadata):

        base = self.metadata_to_outdir(metadata)
        flnm = 'LogFiles/SARSCOV2_{}_aimless_xml.xml'.format(metadata)

        tree = ET.parse(pjoin(base, flnm))
        root = tree.getroot()

        e = root.findall('./Result/Dataset/ResolutionHigh/Overall')

        if len(e) == 1:
            res = e[0].text.strip()
        else:
            raise IOError('2 resolution entries in aimless?')

        return res


    def submit_unfinished(self, limit=None, verbose=True):

        # TODO
        # this code is almost the same as in xiadaemon... can we combine?

        t = time.localtime()
        current_time = time.strftime("%H:%M:%S", t)
        print('')
        print('>> dimpling daemon crunching latest results...')
        print('>>', current_time)

        # get sucessfully completed xia2 runs
        to_run = self.fetch_xia_successes()
        if verbose:
            print('xia2 completed:                 {}'.format(len(to_run)))

        # see which not already finished
        to_rm = []
        for md in to_run:
            if self.xia_result(md) in ['finished', 'procfail']:
                to_rm.append(md)
        to_run = to_run - set(to_rm)
        if verbose:
            print('Processed already:               {}'.format(len(to_rm)))

        # see which not already submitted
        running = set(self.fetch_running_jobs())
        to_run = to_run - running
        if verbose:
            print('Running on SLURM:                {}'.format(len(running)))

        # submit the rest
        if verbose:
            print('Submitting:                      {}'.format(len(to_run)))

        for md in list(to_run)[:limit]:
            try:
                self.submit_run(md)
            except OSError as e:
                # TODO this shouldn't happen, but it is...
                print('! warning !', e)
                print('trying to proceed...')

        return


    def submit_run(self, metadata, debug=False, allow_overwrite=True):

        outdir = self.metadata_to_outdir(metadata)
        resoln = self.get_resolution(metadata)

        # then write and sub the slurm script
        batch_script="""#!/bin/bash

#SBATCH --partition=cfel
#SBATCH --nodes=1
#SBATCH --chdir     {outdir}
#SBATCH --job-name  {name}-dmpl_{metadata}
#SBATCH --output    {name}-dmpl_{metadata}.out
#SBATCH --error     {name}-dmpl_{metadata}.err

export LD_PRELOAD=""
source /etc/profile.d/modules.sh

module load ccp4/7.0
module load phenix/1.13


metadata={metadata}
resolution={resolution}

# >> static references (SHOULD COME FROM SQL!)
# NOTE : TJL TODO
# 
# The following two lines make this project MPro specific
# and highly brittle... we should improve them at the 
# first opportunity
ref_pdb="/asap3/petra3/gpfs/p11/2020/data/11009999/shared/mpro_references/SARS-COV-mpro_refine_110_edited.pdb"
uni_free=/home/tjlane/opt/xia2pipe/scripts/uni_free.csh


# >> inferred input
input_mtz="./DataFiles/SARSCOV2_${{metadata}}_free.mtz"


# >> uni_free : same origin, reset rfree flags
csh ${{uni_free}} ${{input_mtz}} ${{metadata}}_rfree.mtz


# >> cut resolution of MTZ
cut_mtz=${{metadata}}_rfree_rescut.mtz # WORK ON NAME
mtzutils hklin ${{metadata}}_rfree.mtz \
hklout ${{cut_mtz}} <<eof
resolution ${{resolution}}
eof


# >> dimple #1
dimple ${{ref_pdb}} ${{cut_mtz}}       \
  --free-r-flags ${{cut_mtz}}          \
  -f png                               \
  --jelly 0                            \
  --restr-cycles 15                    \
  --hklout ${{metadata}}_dim1_out.mtz  \
  --xyzout ${{metadata}}_dim1_out.pdb  \
  {outdir}

# >> add riding H
phenix.ready_set ${{metadata}}_dim1_out.pdb


# >> phenix refinement
phenix.refine ${{cut_mtz}} ${{metadata}}_dim1_out.updated.pdb           \
  prefix=${{metadata}}                                                  \
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
  ordered_solvent=True                                                  \
  simulated_annealing.start_temperature=2500


# >> dimple #2
dimple ${{metadata}}_002.pdb ${{cut_mtz}} \
  --free-r-flags ${{cut_mtz}}             \
  -f png                                  \
  --jelly 0                               \
  --restr-cycles 15                       \
  --hklout ${{metadata}}_postphenix_out.mtz \
  --xyzout ${{metadata}}_postphenix_out.pdb \
  {outdir}

        """.format(
                    name       = self.name,
                    metadata   = metadata,
                    outdir     = outdir,
                    resolution = resoln,
                  )

        # create a slurm sub script
        # TODO : is this the best directory to make a tmp file?
        slurm_file='/tmp/dmpl-{}-{}.sh'.format(self.name, metadata)
        with open(slurm_file, 'w') as f:
            f.write(batch_script)

        # submit to queue and cleanup
        if not debug:
            r = subprocess.run("/usr/bin/sbatch {}".format(slurm_file),
                               shell=True, check=True)
            os.remove(slurm_file)

        return


if __name__ == '__main__':

    name     = 'DIALS'
    pipeline = 'dials'
    projpath = '/asap3/petra3/gpfs/p11/2020/data/11009999'

    md = 'l9p21_04'

    dd = DimplingDaemon(name, pipeline, projpath)

    #print( dd.get_resolution(md))
    #dd.submit_run(md)

    print('running', dd.fetch_running_jobs() )


