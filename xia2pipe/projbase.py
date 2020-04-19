

import os
from glob import glob
from os.path import join as pjoin


class ProjectBase:


    # TODO
    # this class is technically abstract and should enforce the
    # implementation of the following attributes in all children
    # 
    # name -> str
    # projpath -> str


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


    def xia_result_exists(self, metadata):

        # check for something like this:
        # /asap3/petra3/gpfs/p11/2020/data/11009999/scratch_cc/DIALS/l8p23_03/DataFiles/SARSCOV2_l8p23_03_free.mtz
        # ^ ----------------------- outdir -------------------------------- ^

        outdir = self.metadata_to_outdir(metadata)
        mtzpth = "DataFiles/SARSCOV2_{}_free.mtz".format(metadata)
        full_mtz_path = pjoin(outdir, mtzpth)

        return os.path.exists(full_mtz_path)



