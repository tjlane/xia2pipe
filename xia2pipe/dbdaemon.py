
import time
import argparse

from xia2pipe.projbase import ProjectBase


class DBDaemon(ProjectBase):
    """
    Run through the data and update the SQL DB accordingly
    """

    def in_db(self, metadata, run, table):

        cid = self.metadata_to_id(metadata, run)

        if table == 'Data_Reduction':
            try:
                k = self.get_reduction_id(cid, run)
            except OSError as e:
                k = []

        elif table == 'Refinement':
            try:
                k = self.get_refinement_id(cid, run)
            except OSError as e:
                k = []

        else:
            raise ValueError('`table` must be Data_Reduction, Refinement'
                             ' got: {}'.format(which))

        if k:
            exists = True
        else:
            exists = False

        return exists


    def _update(self, table, list_to_check, data_fetcher, to_file=None):
        """
        table : Data_Reduction or Refinement
        list_to_check : [(md, run), (md, run), ...]
        data_fetcher : self.xia_data, self.dmpl_data
        """

        n_inserted = 0
        n_already  = 0

        for md, run in list_to_check:

            if not self.in_db(md, run, table):

                try:
                    data = data_fetcher(md, run)
                except Exception as e:
                    print('! issue with {} {}'.format(md, run))
                    print(e)
                    continue

                if float('nan') in data.values():
                    print('nan in values!', data)
                    continue

                if to_file:
                    columns = ', '.join(list(data.keys()))
                    values  = ', '.join(["'%s'"%v if v else "NULL" for v in data.values()])
                    to_file.write('INSERT INTO SARS_COV_2_Analysis_v2.{} ({}) VALUES ({});'
                                  '\n'.format(table, columns, values))
                else:
                    self.db.insert('{}.{}'.format(self._analysis_db, table),
                                   data,
                                   verbose=False)
                n_inserted += 1
            else:
                n_already += 1

        print('')
        print('> {:14s} ---'.format(table))
        print('inserted:       {}'.format(n_inserted))
        print('already in db:  {}'.format(n_already))

        return


    def update_xia(self, to_file=None):
        self._update('Data_Reduction',
                     self.fetch_reduction_successes(),
                     self.xia_data,
                     to_file=to_file)
        return


    def update_dimpling(self, to_file=None):
        self._update('Refinement',
                     self.fetch_dmpl_successes(),
                     self.dmpl_data,
                     to_file=to_file)
        return


def script():

    parser = argparse.ArgumentParser(description='update the database with processed values')
    parser.add_argument('config', type=str,
                        help='the configuration yaml file to use')
    parser.add_argument('--outfile', type=str, default=None, required=False,
                        help='write the SQL commands to a file for later upload')
    parser.add_argument('--direct', action='store_true', default=False,
                        help='directly inject results into DB')
    args = parser.parse_args()

    dbd = DBDaemon.load_config(args.config)

    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print('')
    print('>> DB daemon synching latest results...')
    print('>>', current_time)

    if args.outfile:
        print('writing --> {}'.format(args.outfile))
        with open(args.outfile, 'w') as f:
            dbd.update_xia(to_file=f)
            dbd.update_dimpling(to_file=f)

    elif args.direct:
       print('--> direct injection to SQL requested')
       conf = input('    are you sure? [y/n] ')
       if conf in ['y', 'Y', 'yes', 'Yes', 'YES']:
           dbd.update_xia()
           dbd.update_dimpling()

    else:
        raise RuntimeError('must provide `outfile` or set `--direct`')

    return


