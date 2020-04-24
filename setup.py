
from setuptools import setup

setup(name='xia2pipe',
      version='0.1',
      description='A pipeline for automatic xtal processing & refinement',
      url='https://stash.desy.de/projects/X2P/repos/xia2pipe/browse',
      author='TJ Lane',
      author_email='thomas.lane@desy.de',
      packages=['xia2pipe'],
      entry_points = {
          'console_scripts': [
              'x2p.reduce=xia2pipe.xiadaemon:script',
              'x2p.refine=xia2pipe.dmpldaemon:script',
              'x2p.sync=xia2pipe.dbdaemon:script',
          ],
      },
      zip_safe=False)
