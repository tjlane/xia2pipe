

import os
import sys
import yaml
from glob import glob
from os.path import join as pjoin

sys.path.insert(0, "/gpfs/cfel/cxi/common/public/SARS-CoV-2/stable/connector")
sys.path.insert(0, "/gpfs/cfel/cxi/common/public/SARS-CoV-2/stable/database-tools")

from MySQL.dev.connector import SQL
from config import database_config as config



class ProjectBase:

    def __init__(self, 
                 name,
                 pipeline,
                 projpath,
                 spacegroup=None, 
                 unit_cell=None,
                 sql_config=None,
                 slurm_config=None):

        self.name     = name
        self.projpath = projpath

        self.pipeline   = pipeline
        self.spacegroup = spacegroup
        self.unit_cell  = unit_cell

        if pipeline.lower() not in ['dials', '2d', '3d', '3dii']:
            raise ValueError('pipeline: {} not valid'.format(pipeline))

        # ensure output dir exists
        self.pipedir = "/asap3/petra3/gpfs/p11/2020/data/11009999/scratch_cc/{}".format(self.name)
        if not os.path.exists(self.pipedir):
            os.mkdir(self.pipedir)

        # connect to the SARS-COV-2 SQL db
        self.db = SQL(sql_config)

        # save the slurm configuration
        self.slurm_config = slurm_config

        return


    def metadata_to_id(self, metadata):
        cid = self.db.fetch(
            "SELECT crystal_id FROM Diffractions WHERE "
            "metadata='{}';".format(metadata)
        )
        assert len(cid) == 1, cid
        return cid[0]['crystal_id']


    def raw_data_exists(self, metadata):
        pth = pjoin(self.projpath, "raw/{}/*/*.cbf".format(metadata))
        cbfs = glob(pth)
        if len(cbfs) > 20:
            data_exists = True
        else:
            data_exists = False
        return data_exists


    def metadata_to_rawdir(self, metadata):
        """
        Fetch the latest run directory for a given metadata by inspecting
        what is on disk.

        TODO x-ref against database?
        """

        base = pjoin(self.projpath, "raw/{}".format(metadata))
        runs = sorted( os.listdir(base) )
        
        latest_run = pjoin(base, runs[-1])

        return latest_run


    def metadata_to_outdir(self, metadata):
        s = pjoin(self.projpath, 
                  "scratch_cc/{}/{}".format(self.name, metadata))
        return s


    def xia_result(self, metadata):

        # check for something like this:
        # /asap3/petra3/gpfs/p11/2020/data/11009999/scratch_cc/DIALS/l8p23_03/DataFiles/SARSCOV2_l8p23_03_free.mtz
        # ^ ----------------------- outdir -------------------------------- ^

        outdir = self.metadata_to_outdir(metadata)

        mtzpth = "DataFiles/SARSCOV2_{}_free.mtz".format(metadata)
        full_mtz_path = pjoin(outdir, mtzpth)

        errpth = pjoin(outdir, 'xia2.error')

        if os.path.exists(full_mtz_path):
            result = 'finished'
        elif os.path.exists(errpth):
            result = 'procfail'
        else:
            result = 'notdone'

        return result


    def dmpl_result(self, metadata):

        outdir = self.metadata_to_outdir(metadata)

        mtzpth = "{}_postphenix_out.mtz".format(metadata)
        full_mtz_path = pjoin(outdir, mtzpth)

        errpth = pjoin(outdir, 'dmpl_{}.err'.format(metadata))

        if os.path.exists(full_mtz_path):
            result = 'finished'
        elif os.path.exists(errpth):
            result = 'procfail'
        else:
            result = 'notdone'

        return result


    @classmethod
    def load_config(cls, filename):

        config = yaml.safe_load(open(filename, 'r'))

        proj_config = config['project']

        # TODO better error reporting
        name     = proj_config.pop('name')
        pipeline = proj_config.pop('pipeline')
        projdir  = proj_config.pop('projpath')

        return cls(name,
                   pipeline,
                   projdir,
                   sql_config=config['sql'],
                   slurm_config=config['slurm'],
                   **proj_config)



