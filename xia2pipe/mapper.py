
"""
Map a function onto a (finished) pipeline
"""

import os
import sys

from xia2pipe.projbase import ProjectBase


class Mapper(ProjectBase):

    def function(self, metadata, run):
        raise NotImplementedError('abstract base method')


    def is_finished(self, metadata, run):
        raise NotImplementedError('abstract base method')


    def map_to_all(self, check_only=False):

        to_check = self.fetch_dmpl_successes()
        to_run   = []

        print('Checking {} complete datasets...'
              ''.format(len(to_check)))

        for md, run in to_check:
            if not self.is_finished(md, run):
                to_run.append( (md, run) )

        print('... found {} requiring mapping.'
              ''.format(len(to_run)))

        if not check_only:
            for md, run in to_run:
                self.function(md, run)

        return


