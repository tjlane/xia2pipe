#!/usr/bin/env python


import os
import sys
from glob import glob

from xia2pipe.dmpldaemon import DimplingDaemon


def fetch_to_try_again(config_file):

    dd = DimplingDaemon.load_config(config_file)

    to_run = set(dd.fetch_reduction_successes(in_db=True))
    print('completed:                       {}'.format(len(to_run)))

    # see which not already finished
    to_try_again = []
    successes = 0
    failures  = 0
    for md in to_run:
        if dd.dmpl_result(*md) == 'finished':
            successes += 1
        elif dd.dmpl_result(*md) == 'procfail':
            to_try_again.append(md)
            failures  += 1

    print('Processed ({:04d} s/{:04d} f):       {}'
          ''.format(successes, failures, successes+failures))

    to_try_again = set(to_try_again)

    # see which not already submitted
    running = set(dd.fetch_running_jobs())
    to_try_again = to_try_again - running
    print('Running on SLURM:                {}'.format(len(running)))

    print('Will Try Again:                  {}'.format(len(to_try_again)))

    return list(to_try_again)


def remove_err_files(config_file, list_mds, dryrun=False):

    dd = DimplingDaemon.load_config(config_file)

    for metadata, run in list_mds:
        od = dd.metadata_to_outdir(metadata, run)

        errfile_ptrn = os.path.join(od, '*dmpl*.err')
        res = glob(errfile_ptrn)
        if len(res) == 1:
            print('{}_{:03d}\t{}'.format(metadata, run, res[0]))
            if not dryrun:
                os.remove(res[0])

    return


if __name__ == '__main__':

    check = input('Are you sure? [y] ')
    if check != 'y':
        proint('bye.')
        sys.exit(0)

    config_file = sys.argv[-1]
    todo = fetch_to_try_again(config_file)
    remove_err_files(config_file, todo)

