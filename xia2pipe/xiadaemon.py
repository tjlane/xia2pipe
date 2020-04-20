
import os
import sys
import re
import time
import subprocess
from glob import glob

from projbase import ProjectBase


class XiaDaemon(ProjectBase):

    def __init__(self, name, pipeline, projpath, 
                 spacegroup=None, unit_cell=None):

        super().__init__(name, pipeline, projpath,
                         spacegroup=None, unit_cell=None)

        # ensure output dir exists
        self.pipedir = "/asap3/petra3/gpfs/p11/2020/data/11009999/scratch_cc/{}".format(self.name)        
        if not os.path.exists(self.pipedir):
            os.mkdir(self.pipedir)

        return


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
            if not self.raw_data_exists(md):
                to_rm.append(md)
                #print('warning! ds {} in DB but not on disk'.format(md))
        to_run = to_run - set(to_rm)

        if verbose:
            print('Diffracting crystals collected:  {}'.format(len(to_run)))

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


    def fetch_diffraction_successes(self):
        successes = self.db.fetch(
            "SELECT metadata FROM Master_View WHERE diffraction='Success';"
        )
        return [ s['metadata'] for s in successes ]


    def fetch_running_jobs(self):
        """
        Return a list of metadata str that are running on SLURM
        """

        running = []

        r = subprocess.run("/software/tools/bin/slurm queue", 
                           capture_output=True, shell=True, check=True)
        
        lines = r.stdout.decode("utf-8").split('\n')
        for line in lines:
            g = re.search('{}_(\w+)'.format(self.name), line)
            if g:
                running.append(g.groups()[0])

        return running


    def submit_run(self, metadata, debug=False, allow_overwrite=True):

        # first, create the directory sub-structure
        rawdir = self.metadata_to_rawdir(metadata)
        outdir = self.metadata_to_outdir(metadata)

        if not os.path.exists(outdir):
            os.mkdir(outdir)
        else:
            if not allow_overwrite:
                raise IOError('output directory {}/{} already exists...'
                              ''.format(self.name, metadata))
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
#SBATCH --job-name  {name}_{metadata}
#SBATCH --output    {name}_{metadata}.out
#SBATCH --error     {name}_{metadata}.err

export LD_PRELOAD=""
source /etc/profile.d/modules.sh

module load ccp4/7.0

imgs={rawdir}
xia2 pipeline={pipeline} project=SARSCOV2 crystal={metadata} nproc=32 {sgstr} {ucstr} $imgs

        """.format(
                    name     = self.name,
                    metadata = metadata,
                    pipeline = self.pipeline,
                    rawdir   = rawdir,
                    outdir   = outdir,
                    sgstr    = sgstr,
                    ucstr    = ucstr
                  )

        # create a slurm sub script
        # TODO : is this the best directory to make a tmp file?
        slurm_file='/tmp/xia2-{}-{}.sh'.format(self.name, metadata)
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

    xd = XiaDaemon(name, pipeline, projpath)
    xd.submit_unfinished(verbose=True)


