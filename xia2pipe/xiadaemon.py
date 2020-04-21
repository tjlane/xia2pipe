
import os
import sys
import re
import time
import subprocess
from glob import glob

from projbase import ProjectBase


class XiaDaemon(ProjectBase):

    def submit_unfinished(self, verbose=False, limit=None):
        """
        Check:

          -- all crystals labeled success in db
          -- which exist in raw/
          -- which not already finished
          -- which not already submitted

        And submits any missing.
        """

        t = time.localtime()
        current_time = time.strftime("%H:%M:%S", t)
        print('')
        print('>> xia2 daemon crunching latest results...')
        print('>>', current_time)

        # fetch all xtals labeled success in db
        to_run = set(self.fetch_diffraction_successes())
        if verbose:
            print('Fetched from database:           {}'.format(len(to_run)))

        to_rm = []
        for md in to_run:
            if not self.raw_data_exists(*md):
                to_rm.append(md)
                #print('warning! ds {} in DB but not on disk'.format(md)) # TODO
        to_run = to_run - set(to_rm)

        if verbose:
            print('Diffracting crystals collected:  {}'.format(len(to_run)))

        # see which not already finished
        to_rm = []
        successes = 0
        failures  = 0
        for md in to_run:
            if self.xia_result(*md) == 'finished':
                to_rm.append(md)
                successes += 1
            elif self.xia_result(*md) == 'procfail':
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


    def fetch_diffraction_successes(self):
        successes = self.db.fetch(
            "SELECT metadata, run_id FROM Master_View WHERE diffraction='Success';"
        )
        return [ (s['metadata'], s['run_id']) for s in successes ]


    def fetch_running_jobs(self):
        """
        Return a list of metadata str that are running on SLURM
        """

        running = []

        r = subprocess.run('sacct --format="JobID,JobName%30" --state="RUNNING,PENDING"',
                           capture_output=True, shell=True, check=True)
        
        lines = r.stdout.decode("utf-8").split('\n')
        for line in lines:
            g = re.search('{}_(\w+)-(\d)'.format(self.name), line)
            if g:
                grps = g.groups()
                running.append( (grps[0], int(grps[1])) ) # metadata, run_id

        return running


    def submit_run(self, metadata, run, debug=False, allow_overwrite=True):

        # first, create the directory sub-structure
        rawdir = self.metadata_to_rawdir(metadata, run)
        outdir = self.metadata_to_outdir(metadata, run)

        if not os.path.exists(outdir):
            os.makedirs(outdir)
        else:
            if not allow_overwrite:
                raise IOError('output directory already exists...')
            # if we allow overwrite, just continue...

        # optionally make flags setting the SG and UC
        if self.spacegroup is not None:
            sgstr = 'spacegroup={}'.format(self.spacegroup)
        else:
            sgstr = ''

        if self.unit_cell is not None:
            ucstr = 'unit_cell={}'.format(self.unit_cell)
        else:
            ucstr = ''

#SBATCH --partition=cfel

#SBATCH --partition=all
#SBATCH --reservation=covid

        # then write and sub the slurm script
        batch_script="""#!/bin/bash

#SBATCH --partition=cfel
#SBATCH --nodes=1
#SBATCH --chdir     {outdir}
#SBATCH --job-name  {name}_{metadata}-{run}
#SBATCH --output    {name}_{metadata}-{run}.out
#SBATCH --error     {name}_{metadata}-{run}.err

export LD_PRELOAD=""
source /etc/profile.d/modules.sh

module load ccp4/7.0

imgs={rawdir}
xia2 pipeline={pipeline} project=SARSCOV2 crystal={metadata}_{run:03d} nproc=32 {sgstr} {ucstr} $imgs

        """.format(
                    name     = self.name,
                    metadata = metadata,
                    run      = run,
                    pipeline = self.pipeline,
                    rawdir   = rawdir,
                    outdir   = outdir,
                    sgstr    = sgstr,
                    ucstr    = ucstr
                  )

        # create a slurm sub script
        # TODO : is this the best directory to make a tmp file?
        slurm_file='/tmp/xia2-{}-{}-{}.sh'.format(self.name, metadata, run)
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


if __name__ == '__main__':

    xd = XiaDaemon.load_config('config.yaml')

    #ds = xd.fetch_diffraction_successes()
    #print(len(ds), ds[:3])

    xd.submit_unfinished(verbose=True, limit=None)

    #time.sleep(2)
    #print( len(xd.fetch_running_jobs()) )

