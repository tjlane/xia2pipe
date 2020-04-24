

import os
import sys
import re
import yaml
import json
import ast
import time
import configparser
import subprocess

from glob import glob
from datetime import datetime
from os.path import join as pjoin

from xia2pipe.connector import SQL, get_single


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

    if not g:
        raise RuntimeError('could not find b-factor in output of'
                           'phenix.b_factor_statistics')
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
                 results_dir,
                 rawdata_dirs=[], # can use database instead
                 reference_pdb=None,
                 sql_config=None,
                 slurm_config=None,
                 xia2_config=None):

        self.name          = name
        self.pipeline      = pipeline
        self.results_dir   = results_dir
        self.rawdata_dirs  = rawdata_dirs
        self.reference_pdb = reference_pdb

        if pipeline.lower() not in ['dials', '2d', '3d', '3dii']:
            raise ValueError('pipeline: {} not valid'.format(pipeline))

        # ensure output dir exists
        if not os.path.exists(self.results_dir):
            print('Creating: {}'.format(self.results_dir))
            os.makedirs(self.results_dir)

        # connect to the SARS-COV-2 SQL db
        self.db = SQL(sql_config)

        # save the slurm, xia2 configuration
        self.slurm_config = slurm_config
        self.xia2_config  = xia2_config

        return


    @property
    def method_name(self):
        return '{}-{}'.format(self.name, self.pipeline)


    def metadata_to_id(self, metadata, run):
        cid = self.db.select('crystal_id',
                             'SARS_COV_2_v2.Diffractions',
                             {'metadata' : metadata, 'run_id' : run},
                             )
        if len(cid) == 0:
            raise IOError('no crystal_id in database for '
                          'metadata={}, run={}'.format(metadata, run))
        if len( set([ x['crystal_id'] for x in cid ]) ) > 1: # check unique
            print(cid)
            raise IOError('found multiple irreconcilable crystal_id`s in db for '
                          'metadata={}, run={}'.format(metadata, run))
        return cid[0]['crystal_id']


    def id_to_metadata(self, crystal_id, run):
        md = self.db.select('metadata',
                            'SARS_COV_2_v2.Diffractions',
                            {'crystal_id' : crystal_id, 'run_id' : run})
        return get_single(md, crystal_id, run, 'metadata')


    def get_reduction_id(self, crystal_id, run):
        """
        We assume one name/pipeline combo results in a unique reduction
        id per crystal.
        """
        qid = self.db.select('data_reduction_id',
                             'SARS_COV_2_Analysis_v2.Data_Reduction',
                             {'crystal_id' : crystal_id, 'run_id' : run,
                             'method' : self.method_name })
        return get_single(qid, crystal_id, run, 'data_reduction_id') 


    def get_refinement_id(self, crystal_id, run):
        """ 
        We assume one name/pipeline combo results in a unique refinement
        id per crystal.
        """
        # probably dont need this function TODO
        data_reduction_id = self.get_reduction_id(crystal_id, run)
        qid = self.db.select('refinement_id',
                             'SARS_COV_2_Analysis_v2.Refinement',
                             {'data_reduction_id' : data_reduction_id, 
                              'run_id' : run,
                              'method' : (self.method_name + '-dmpl')})
        return get_single(qid, crystal_id, run, 'refinement_id') 


    def metadata_to_dataset_path(self, metadata, run, skip_db=False):
        """
        Fetch the latest run directory for a given metadata by inspecting
        what is on disk.

        Specifically, this function returns the entire path to e.g. cbf files
        not a generic dataset directory.

        Repeated database queries are going slowly at the moment, so you
        can switch that off with skip_db=True.
        """

        # >> try to use the database
        if not skip_db:

            crystal_id = self.metadata_to_id(metadata, run)

            dp_qry = self.db.select('data_raw_filename_pattern',
                                    'SARS_COV_2_v2.Diffractions',
                                    {'crystal_id' : crystal_id, 'run_id' : run})
            data_pattern = get_single(dp_qry, crystal_id, run, 'data_raw_filename_pattern')

        else:
            data_pattern = None


        if data_pattern: # is not None, aka database success
            dataset_path = os.path.dirname(data_pattern)
            ext          = os.path.basename(s).split('.')[-1]

        # >> but if that fails, fall back on searching directories
        else: # data_pattern is None        

            possible_dirs = []
            for rawdata_dir in self.rawdata_dirs:
                dataset_path = pjoin(rawdata_dir,
                                     "{}/{}_{:03d}/".format(metadata, metadata, run))
                if os.path.exists(dataset_path):
                    possible_dirs.append(dataset_path)

            if len(possible_dirs) == 0:
                raise IOError('cannot find data for {}_{:03d}'.format(metadata, run))
            elif len(possible_dirs) > 1:
                dataset_path = possible_dirs[0]
                print(' ! warning ! found >1 data directory for:')
                print('{}_{:03d}'.format(metadata, run))
                print('using first: {}'.format(rawdir))
            else:
                dataset_path = possible_dirs[0]
                ext = '.cbf' # TODO be smarter

        return dataset_path, ext


    def raw_data_exists(self, metadata, run, min_files=800):

        try:
            dataset_path, ext = self.metadata_to_dataset_path(metadata, run)
        except IOError as e:
            #print(e)
            return False # for now assume data do not exist

        pth = dataset_path + "*{}".format(ext)
        n_files = len(glob(pth))

        if n_files > min_files:
            data_exists = True
        else:
            data_exists = False

        return data_exists


    def metadata_to_outdir(self, metadata, run):
        s = pjoin(self.results_dir, 
                  "{}/{}/{}_{:03d}".format(self.name, 
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
        errpth = pjoin(outdir, 'xia2.error')

        # note : mtz path is fixed in xia_data() as well...
        #        not ideal software engineering but OK for now
        mtz_path = pjoin(outdir,
                         "DataFiles/SARSCOV2_{}_{:03d}_free.mtz".format(metadata, run))

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
                    'crystal_id' :   self.metadata_to_id(metadata, run),
                    'run_id':        run,
                    'analysis_time': filetime(mtz_path),
                    'folder_path':   outdir,
                    'mtz_path':      mtz_path,
                    'method':        self.method_name,
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


    def fetch_xia_successes(self):

        successes = self.db.select('metadata, run_id',
                                   'SARS_COV_2_v2.Diffractions',
                                   {'diffraction' : 'Success'})

        to_run = []
        for md in [ (s['metadata'], s['run_id']) for s in successes ]:
            if self.xia_result(*md) == 'finished':
                to_run.append(md)

        return to_run


    def get_resolution(self, metadata, run):
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

        cid = self.metadata_to_id(metadata, run)
        outdir = self.metadata_to_outdir(metadata, run)

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
                     'data_reduction_id':    get_reduction_id(cid, run),
                     'analysis_time':        filetime(mtz_path),
                     'folder_path':          outdir,
                     'initial_pdb_path':     self.reference_pdb,
                     'final_pdb_path':       pdb_path,
                     'refinement_mtz_path':  mtz_path,
                     'method':               self.method_name + '-dmpl',
                     'resolution_cut':       self.get_resolution(metadata, run),
                     'rfree':                fmt(log['refmac5 restr']['free_r']),
                     'rwork':                fmt(log['refmac5 restr']['overall_r']),
                     'rms_bond_length':      fmt(log['refmac5 restr']['rmsbond']),
                     'rms_bond_angle':       fmt(log['refmac5 restr']['rmsangl']),
                     'num_blobs':            _count_blobs(log),
                     'average_model_b':      _get_average_model_b(pdb_path),
                    }
        
        return data_dict


    def fetch_dmpl_successes(self):

        successes = self.db.select('metadata, run_id',
                                   'SARS_COV_2_v2.Diffractions',
                                   {'diffraction' : 'Success'})

        to_run = []
        for md in [ (s['metadata'], s['run_id']) for s in successes ]:
            if self.dmpl_result(*md) == 'finished':
                to_run.append(md)

        return to_run


    @classmethod
    def load_config(cls, filename):

        config = yaml.safe_load(open(filename, 'r'))

        proj_config = config['project']

        try:
            name         = proj_config.pop('name')
            pipeline     = proj_config.pop('pipeline')
            results_dir  = proj_config.pop('results_dir')
        except KeyError as e:
            raise IOError('Missing required parameter in config.yaml\n', e)

        return cls(name,
                   pipeline,
                   results_dir,
                   sql_config=config.get('sql', None),
                   slurm_config=config.get('slurm', None),
                   xia2_config=config.get('xia2', None),
                   **proj_config)


if __name__ == '__main__':

    pb = ProjectBase.load_config('../configs/DIALS.yaml')

    print(pb.fetch_dmpl_successes())

    for md,run in [('l9p05_06', 1), ('l4p23_05', 1)]:
        pass
        #print(pb.metadata_to_id(md, run))
        #print(pb.raw_data_exists(md, run))
        #print(pb.metadata_to_dataset_path(md, run))

        #cid = pb.metadata_to_id(md, run)
        #print(pb.get_reduction_id(cid, run))
        #print(pb.get_refinement_id(cid, run))

        #print(pb.xia_data(md, run))
        #print(pb.dmpl_data(md, run))

