
import os
import sys
import subprocess

from os.path import join as pjoin

sys.path.insert(0, "/gpfs/cfel/cxi/common/public/SARS-CoV-2/stable/connector") # TODO
sys.path.insert(0, "/gpfs/cfel/cxi/common/public/SARS-CoV-2/stable/database-tools")

from MySQL.dev.connector import SQL
from config import database_config as config


class XiaDaemon:

    def __init__(self, name, pipeline, spacegroup=None, unit_cell=None):
        """
        name : str that identifies this pipeline
        """

        if pipeline.lower() not in ['dials', '2d', '3d', '3dii']:
            raise ValueError('pipeline: {} not valid'.format(pipeline))

        self.name       = name
        self.pipeline   = pipeline
        self.spacegroup = spacegroup
        self.unit_cell  = unit_cell

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


    def fetch_diffraction_successes(self):
        successes = self.db.fetch(
            "SELECT metadata FROM Master_View WHERE diffraction='Success';"
        )
        return successes


    def fetch_running_jobs(self):
        return


    @staticmethod
    def raw_data_exists(metadata):
        return


    @staticmethod
    def xia_result_exists(metadata):
        return


    @property
    def crystals_to_run(self):
        """
        Get list of crystals to run

          -- success in db
          -- exists in raw
          -- not already submitted
          -- not already finished

        Returned as a list of str, where the str are metadata entries.
        """
        return



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
#SBATCH --job-name  {pipeline}_{metadata}
#SBATCH --output    {pipeline}_{metadata}.out
#SBATCH --error     {pipeline}_{metadata}.err

export LD_PRELOAD=""
source /etc/profile.d/modules.sh

module load ccp4/7.0

imgs={rawdir}
xia2 pipeline={pipeline} project=SARSCOV2 crystal={metadata} nproc=32 {sgstr} {ucstr} $imgs

        """.format(
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

    xd = XiaDaemon('DIALS', 'dials')
    #xd.submit_run('l8p23_03', debug=False)    

    print(xd.fetch_diffraction_successes())
