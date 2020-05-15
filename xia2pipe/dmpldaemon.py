
import os
import re
import time
import subprocess
import argparse
from os.path import join as pjoin

from xia2pipe.projbase import ProjectBase




class DimplingDaemon(ProjectBase):


    def fetch_running_jobs(self):
        """
        Return a list of metadata str that are running on SLURM
        """

        running = []

        r = subprocess.run('sacct --format="JobID,JobName%50" --state="RUNNING,PENDING"',
                           capture_output=True, shell=True, check=True)

        lines = r.stdout.decode("utf-8").split('\n')
        for line in lines:
            g = re.search('{}-dmpl_(\w+)-(\d)'.format(self.name), line)
            if g:
                grps = g.groups()
                running.append( (grps[0], int(grps[1])) ) # metadata, run_id

        return running


    def fetch_input_mtz(self, metadata, run):
        """
        Lookup the mtz output from the 'Reductions' table
        """

        cid = self.metadata_to_id(metadata, run)

        qry = self.db.select(
                             'mtz_path',
                             '{}.Data_Reduction'.format(self._analysis_db),
                              {
                                'crystal_id': cid, 
                                'run_id': run, 
                                'method': self.method_name,
                              },
                            )

        if len(qry) == 0:
            if self.pipeline == 'dials':
                # this should work as a default
                i_mtz = "./DataFiles/SARSCOV2_{}_free.mtz".format(metadata)
            else:
                print('using method:', self.method_name)
                raise RuntimeError('cannot find `mtz_path` in db for:\n'
                                   '(crystal_id, metadata, run) '
                                   '{}, {}, {}'.format(cid, metadata, run))

        elif len(qry) == 1:
            i_mtz = qry[0]['mtz_path']

        else:
            raise RuntimeError('multiple entries in SQL found, '
                               'should be only one', qry)
        
        return i_mtz


    def submit_unfinished(self, limit=None, verbose=True):

        # TODO
        # this code is almost the same as in xiadaemon... can we combine?

        t = time.localtime()
        current_time = time.strftime("%H:%M:%S", t)
        print('')
        print('>> dimpling daemon crunching latest results...')
        print('>>', current_time)

        # get sucessfully completed xia2 runs
        to_run = set(self.fetch_reduction_successes(in_db=True))
        if verbose:
            print('xia2 completed:                  {}'.format(len(to_run)))

        # see which not already finished
        to_rm = []
        successes = 0
        failures  = 0
        for md in to_run:
            if self.dmpl_result(*md) == 'finished':
                to_rm.append(md)
                successes += 1
            elif self.dmpl_result(*md) == 'procfail':
                to_rm.append(md)
                failures  += 1
        to_run = to_run - set(to_rm)
        if verbose:
            print('Processed ({:04d} s/{:04d} f):       {}'
                  ''.format(successes, failures, len(to_rm)))

        # see which not already submitted
        running = set(self.fetch_running_jobs())
        to_run = to_run - running
        if verbose:
            print('Running on SLURM:                {}'.format(len(running)))

        # submit the rest
        if verbose:
            print('Submitting:                      {}'.format(len(to_run)))

        for md in list(to_run)[:limit]:
            self.submit_run(*md)

        return


    def submit_run(self, metadata, run, debug=False, allow_overwrite=True):

        if not hasattr(self, 'reference_pdb'):
            raise AttributeError('reference_pdb field not set! This is'
                                 'almost certainly a bug in the code.')

        outdir = self.metadata_to_outdir(metadata, run)
        if not os.path.exists(outdir):
            os.makedirs(outdir)

        if self.refinement_config.get('place_waters', True):
            water_str = ''
        else:
            water_str = '--dont-place-waters'


        # then write and sub the slurm script
        batch_script="""#!/bin/bash

#SBATCH --partition={partition}
#SBATCH --reservation={rsrvtn}
#SBATCH --nodes=1
#SBATCH --oversubscribe
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=5
#SBATCH --time=8:00:00
#SBATCH --job-name  {name}-dmpl_{metadata}-{run}
#SBATCH --output    {outdir}/{name}-dmpl_{metadata}-{run}.out
#SBATCH --error     {outdir}/{name}-dmpl_{metadata}-{run}.err

export LD_PRELOAD=""
source /etc/profile.d/modules.sh

module load ccp4/7.0
#module load phenix/1.18 # real_space_refine has bug
source /home/tjlane/opt/phenix/phenix-1.18-3861/phenix_env.sh

/home/tjlane/opt/xia2pipe/scripts/dmpl.sh \
  --dir={outdir}                  \
  --metadata={metadata}_{run:03d} \
  --resolution={resolution}       \
  --refpdb={reference_pdb}        \
  --mtzin={input_mtz}             \
  --freemtz={free_mtz}            \
  {water_flag}               

""".format(
                    name            = self.name,
                    partition       = self.slurm_config.get('partition', 'all'),
                    rsrvtn          = self.slurm_config.get('reservation', ''),
                    metadata        = metadata,
                    outdir          = outdir,
                    run             = run,
                    resolution      = self.get_resolution(metadata, run),
                    input_mtz       = self.fetch_input_mtz(metadata, run),
                    reference_pdb   = self.reference_pdb,
                    free_mtz        = self.refinement_config.get('free_flag_mtz', ''),
                    water_flag      = water_str,
                  )

        # create a slurm sub script
        slurm_file='/tmp/dmpl-{}-{}-{}.sh'.format(self.name, metadata, run)
        with open(slurm_file, 'w') as f:
            f.write(batch_script)

        # submit to queue and cleanup
        if not debug:
            r = subprocess.run("/usr/bin/sbatch {}".format(slurm_file),
                               shell=True,
                               check=True,
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
            os.remove(slurm_file)

        return


def script():

    parser = argparse.ArgumentParser(description='Submit new refinement jobs.')
    parser.add_argument('config', type=str,
                        help='the configuration yaml file to use')
    parser.add_argument('--limit', type=int, default=None,
                        help='max number of jobs to submit')
    args = parser.parse_args()

    dd = DimplingDaemon.load_config(args.config)
    dd.submit_unfinished(verbose=True, limit=args.limit)

    return


if __name__ == '__main__':

    #metadata  = 'MPro_2764_1'
    #run = 1

    jobs = [ 
             ('MPro_4468_0', 1),
             ('l11p18_14',   1),
             ('MPro_2750_0', 1),
             ('MPro_2764_1', 1),
             ('l11p17_11',   1),
           ] 

    dd = DimplingDaemon.load_config('../configs/tst-d1p7.yaml')

    #dd.submit_run('l11p17_11',   1)

    for md, run in jobs:
        print(md, run)
        dd.submit_run(md, run)


