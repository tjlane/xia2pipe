


import os
import sys
import re
import time
import subprocess
from glob import glob

from projbase import ProjectBase


class Master(ProjectBase):

    def __init__(self, name, pipeline, projpath,
                     spacegroup=None, unit_cell=None):

        super().__init__(name, pipeline, projpath,
                         spacegroup=None, unit_cell=None)

        self.update_interval = update_interval

        # ensure output dir exists
        self.pipedir = "/asap3/petra3/gpfs/p11/2020/data/11009999/scratch_cc/{}".format(self.name)
        if not os.path.exists(self.pipedir):
            os.mkdir(self.pipedir)

        print("")
        print(" ~~~ starting xia2 pipeline ~~~")
        print("Name         ", self.name)
        print("Pipeline:    ", self.pipeline)
        print("SG:          ", self.spacegroup)
        print("UC:          ", self.unit_cell)
        print("dir:         ", self.pipedir)
        print("")
