

import os
import sys
import re
import yaml
import json
import ast
import configparser
import subprocess

from glob import glob
from datetime import datetime
from os.path import join as pjoin

sys.path.insert(0, "/gpfs/cfel/cxi/common/public/SARS-CoV-2/stable/connector")
from MySQL.dev.connector import SQL


def _count_blobs(log_handle):
    """
    Return the number of blobs found by dimple, as 
    reported by the dimple log file. The argument of
    this function is a dimple log opened with configparser
    """
    if 'find-blobs' in log_handle.keys():
        blob_info = ast.literal_eval(log_handle['find-blobs']['blobs'])
        n_blobs = len(blob_info)
    else:
        n_blobs = 0
    return n_blobs


def _get_average_model_b(pdb_path):

    ret = subprocess.run('phenix.b_factor_statistics {}'.format(pdb_path),
                         capture_output=True,
                         shell=True)

    if ret.returncode != 0:
        raise RuntimeError('could not run phenix.b_factor_statistics'
                           '- check: your have phenix in your environment'
                           '- check: {} exists'.format(pdb_path))

    p = '\| all     :\s+\d+\s+\d+\s+(\d+.\d+)\s+(\d+.\d+)\s+(\d+.\d+)'
    g = re.search(p, ret.stdout.decode("utf-8"))

    if not len(g.groups()) == 3:
        raise RuntimeError('could not find b-factor in output of'
                           'phenix.b_factor_statistics')
    avg_model_b = float(g.groups()[2])

    return avg_model_b



def filetime(path):
    tstmp = os.path.getmtime(path)
    return datetime.fromtimestamp(tstmp).strftime('%Y-%m-%d %H:%M:%S')


class ProjectBase:
    """
    Note: this class does NOT check data (on disk) or database
          integrity, even though it provides a lot of information
          about such data. Those functions are the responsability
          of the dbdaemon.
    """

    def __init__(self, 
                 name,
                 pipeline,
                 projpath,
                 spacegroup=None, 
                 unit_cell=None,
                 reference_pdb=None,
                 sql_config=None,
                 slurm_config=None):

        self.name     = name
        self.projpath = projpath

        self.pipeline      = pipeline
        self.spacegroup    = spacegroup
        self.unit_cell     = unit_cell
        self.reference_pdb = reference_pdb

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
        #assert len(cid) == 1, cid
        return cid[0]['crystal_id']


    def raw_data_exists(self, metadata, run):
        pth = pjoin(self.projpath, "raw/{}/*_{:03d}/*.cbf".format(metadata, run))
        cbfs = glob(pth)
        if len(cbfs) > 20:
            data_exists = True
        else:
            data_exists = False
        return data_exists


    def metadata_to_rawdir(self, metadata, run):
        """
        Fetch the latest run directory for a given metadata by inspecting
        what is on disk.

        TODO x-ref against database?
        """
        rawdir = pjoin(self.projpath, 
                       "raw/{}/{}_{:03d}".format(metadata, metadata, run))
        return rawdir


    def metadata_to_outdir(self, metadata, run):
        s = pjoin(self.projpath, 
                  "scratch_cc/{}/{}/{}_{:03d}".format(self.name, 
                                                      metadata,
                                                      metadata,
                                                      run))
        return s


    def xia_result(self, metadata, run):

        # check for something like this:
        # / ... /DIALS/l8p23_03/l8p23_03_001/DataFiles/<...>.mtz
        # / ... /DIALS/l8p23_03/l8p23_03_001/<...>.error
        # ^ ---------- outdir ------------ ^

        outdir = self.metadata_to_outdir(metadata, run)
        mtz_path = self.xia_data(metadata, run)['mtz_path']
        errpth = pjoin(outdir, 'xia2.error')

        if os.path.exists(mtz_path):
            result = 'finished'
        elif os.path.exists(errpth):
            result = 'procfail'
        else:
            result = 'notdone'

        return result


    def xia_data(self, metadata, run):

        outdir = self.metadata_to_outdir(metadata, run)

        json_path = pjoin(outdir, 
                          '{}_{:03d}'.format(metadata, run), 
                          'scale/xia2.json')

        if not os.path.exists(json_path):

            # there were a few old datasets processed with just
            # the meadata field, so try and find those -- if 
            # that fails, we fail completely
            orig_json_path = json_path
            json_path = pjoin(outdir, 
                              '{}'.format(metadata), 
                              'scale/xia2.json')
            #print('cannot find: {}, trying: {}'
            #      ''.format(orig_json_path, json_path))

            if not os.path.exists(json_path):
                raise IOError('cannot find DIALS json for '
                              '{}_{:03d}'.format(metadata, run))


        # >>> parse the DIALS json
        root = json.load(open(json_path, 'r'))

        mtz_path = pjoin(outdir,
                         "DataFiles/SARSCOV2_{}_{:03d}_free.mtz".format(metadata, run))

        # here, cell = [a, b, c, alpha, beta, gamma]
        cell = root['_scalr_cell']

        # this one has some strange float-like key, but there is only one
        k = list(root['_scalr_integraters'])[0]
        space_group = root['_scalr_integraters'][k]['_intgr_spacegroup_number']

        # these are keyed by something nasty like '["SARSCOV2", "l6p17_10", "NATIVE"]'
        # but we expect just one sub-directory, so grab that...
        # we want the first entry, which is the entire resolution range
        # the other two are low & high res reflections only
        ss = list(root['_scalr_statistics'].values())[0]

        # >>> format the output
        data_dict = {
                    'crystal_id' :   self.metadata_to_id(metadata),
                    'run_id':        run,
                    'analysis_time': filetime(mtz_path),
                    'folder_path':   outdir,
                    'mtz_path':      mtz_path,
                    'method':        '{}-{}'.format(self.name, self.pipeline),
                    'resolution_cc': ss['High resolution limit'][0],
                    'a':             cell[0],
                    'b':             cell[1],
                    'c':             cell[2],
                    'alpha':         cell[3],
                    'beta':          cell[4],
                    'gamma':         cell[5],
                    'space_group':   space_group,
                    'isigi':         ss['I/sigma'][0],
                    'rmeas':         ss['Rmeas(I)'][0],
                    'cchalf':        ss['CC half'][0],
                    #'rfactor':       None, # TODO
                    'wilson_b':      ss['Wilson B factor'][0],
                    }

        return data_dict


    def get_resolution(self, metadata, run):
            """
            """
            # this function is here to allow later modification of,
            # for example, the resolution cut between the reduction
            # and refinement stages of the pipeline
            # TODO : think about the code structure here
            return self.xia_data(metadata, run)['resolution_cc']


    def dmpl_result(self, metadata, run):

        outdir = self.metadata_to_outdir(metadata, run)

        mtzpth = "{}_{:03d}_postphenix_out.mtz".format(metadata, run)
        full_mtz_path = pjoin(outdir, mtzpth)

        errpth = pjoin(outdir, 'DIALS-dmpl_{}_{:03d}-{}.err'.format(metadata, run, run))

        if os.path.exists(full_mtz_path):
            result = 'finished'
        elif os.path.exists(errpth):
            result = 'procfail'
        else:
            result = 'notdone'

        return result


    def dmpl_data(self, metadata, run):

        # a de-formatting function for the dimple log file
        # -1 here grabs the last round of REFMAC refinement
        fmt = lambda f : float(f.split(',')[-1].strip(', []'))

        outdir = self.metadata_to_outdir(metadata, run)
        method_name = '{}-{}-dmpl'.format(self.name, self.pipeline)

        mtz_name = "{}_{:03d}_postphenix_out.mtz".format(metadata, run)
        mtz_path = pjoin(outdir, mtz_name)

        pdb_name = "{}_{:03d}_postphenix_out.pdb".format(metadata, run)
        pdb_path = pjoin(outdir, pdb_name)

        log_path = pjoin(outdir, 'dimple.log')
        if not os.path.exists(log_path):
            raise IOError('{}_:03d{}/dimple.log does not exist'
                          ''.format(metadata, run))

        # it turns out the python config parser handles dimple.log
        log = configparser.ConfigParser()
        log.read(log_path)

        data_dict = {
                     #'data_reduction_id':
                     'analysis_time':        filetime(mtz_path),
                     'folder_path':          outdir,
                     'initial_pdb_path':     self.reference_pdb,
                     'final_pdb_path':       pdb_path,
                     'refinement_mtz_path':  mtz_path,
                     'method':               method_name,
                     'resolution_cut':       self.get_resolution(metadata, run),
                     'rfree':                fmt(log['refmac5 restr']['free_r']),
                     'rwork':                fmt(log['refmac5 restr']['overall_r']),
                     'rms_bond_length':      fmt(log['refmac5 restr']['rmsbond']),
                     'rms_bond_angle':       fmt(log['refmac5 restr']['rmsangl']),
                     'num_blobs':            _count_blobs(log),
                     'average_model_b':      _get_average_model_b(pdb_path),
                    }
        
        return data_dict


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


if __name__ == '__main__':



    pb = ProjectBase.load_config('config.yaml')

    for md,run in [('l9p05_06', 1), ('l4p23_05', 1)]:
        print(pb.metadata_to_id(md))
        print(pb.raw_data_exists(md, run))
        print(pb.metadata_to_rawdir(md, run))

        print(pb.xia_data(md, run))
        print(pb.dmpl_data(md, run))
