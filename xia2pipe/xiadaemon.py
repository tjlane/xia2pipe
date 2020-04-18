
import os
import sys
import re
import time
import subprocess

from glob import glob
from os.path import join as pjoin

sys.path.insert(0, "/gpfs/cfel/cxi/common/public/SARS-CoV-2/stable/connector") # TODO
sys.path.insert(0, "/gpfs/cfel/cxi/common/public/SARS-CoV-2/stable/database-tools")

from MySQL.dev.connector import SQL
from config import database_config as config


class XiaDaemon:

    def __init__(self, name, pipeline, spacegroup=None, unit_cell=None,
                 update_interval=60):
        """
        name : str that identifies this pipeline
        """

        if pipeline.lower() not in ['dials', '2d', '3d', '3dii']:
            raise ValueError('pipeline: {} not valid'.format(pipeline))

        self.name       = name
        self.pipeline   = pipeline
        self.spacegroup = spacegroup
        self.unit_cell  = unit_cell

        self.update_interval = update_interval

        # connect to the SARS-COV-2 SQL db
        self.db = SQL(config)

        # ensure output dir exists
        pipedir = "/asap3/petra3/gpfs/p11/2020/data/11009999/scratch_cc/{}".format(self.name)        
        if not os.path.exists(pipedir):
            os.mkdir(pipedir)

        print("")
        print(" ~~~ starting xia2 daemon ~~~")
        print("Name         ", name)
        print("Pipeline:    ", self.pipeline)
        print("SG:          ", self.spacegroup)
        print("UC:          ", self.unit_cell)
        print("dir:         ", pipedir)
        print("")

        return


    def start(self):
        """
        Start the daemon, who every interval checks:

          -- all crystals labeled success in db
          -- which exist in raw/
          -- which not already finished
          -- which not already submitted

        And submits any missing.
        """

        print('>> starting daemon with {} sec interval'.format(self.update_interval))

        while True:
        
            t = time.localtime()
            current_time = time.strftime("%H:%M:%S", t)
            print('')
            print('>> checking for latest results...')
            print(current_time)

            self.submit_unfinished(verbose=True)

            # sleep and start again
            time.sleep(self.update_interval)

        return


    def submit_unfinished(self, verbose=False, limit=None):

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
            if self.xia_result_exists(md):
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
            self.submit_run(md)

        return


    def fetch_diffraction_successes(self):
        successes = self.db.fetch(
            "SELECT metadata FROM Master_View WHERE diffraction='Success';"
        )
        return [ s['metadata'] for s in successes ]


    @staticmethod
    def raw_data_exists(metadata):
        cbfs = glob("/asap3/petra3/gpfs/p11/2020/data/11009999/raw/{}/*/*.cbf".format(metadata))
        if len(cbfs) > 20:
            data_exists = True
        else:
            data_exists = False
        return data_exists


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


    def xia_result_exists(self, metadata):

        # check for something like this:
        # /asap3/petra3/gpfs/p11/2020/data/11009999/scratch_cc/DIALS/l8p23_03/DataFiles/SARSCOV2_l8p23_03_free.mtz
        # ^ ----------------------- outdir -------------------------------- ^
    
        outdir = self.metadata_to_outdir(metadata)
        mtzpth = "DataFiles/SARSCOV2_{}_free.mtz".format(metadata)
        full_mtz_path = pjoin(outdir, mtzpth)

        return os.path.exists(full_mtz_path)


    @staticmethod
    def metadata_to_rawdir(metadata):
        """
        Fetch the latest run directory for a given metadata by inspecting
        what is on disk.

        TODO x-ref against database?
        """

        base = "/asap3/petra3/gpfs/p11/2020/data/11009999/raw/{}".format(metadata)
        runs = sorted( os.listdir(base) )
        
        latest_run = pjoin(base, runs[-1])

        return latest_run


    def metadata_to_outdir(self, metadata):
        s = "/asap3/petra3/gpfs/p11/2020/data/11009999/scratch_cc/{}/{}"
        return s.format(self.name, metadata)


    def submit_run(self, metadata, debug=False):

        # first, create the directory sub-structure
        rawdir = self.metadata_to_rawdir(metadata)
        outdir = self.metadata_to_outdir(metadata)

        if not os.path.exists(outdir):
            os.mkdir(outdir)
        else:
            raise IOError('output directory {}/{} already exists...'
                          ''.format(self.name, metadata))

        # optionally make flags setting the SG and UC
        if self.spacegroup is not None:
            sgstr = 'spacegroup={}'.format(self.spacegroup)
        else:
            sgstr = ''

        if self.unit_cell is not None:
            ucstr = 'unit_cell={}'.format(self.unit_cell)
        else:
            ucstr = ''


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
            r = subprocess.run("/usr/bin/sbatch {}".format(slurm_file), shell=True, check=True)
            os.remove(slurm_file)

        return


if __name__ == '__main__':

    xd = XiaDaemon('DIALS', 'dials', update_interval=180)
    xd.start()
    #xd.submit_unfinished(limit=5)


