

from projbase import ProjectBase


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


    def _update(self, table, list_to_check, data_fetcher):
        """
        table : Data_Reduction or Refinement
        list_to_check : [(md, run), (md, run), ...]
        data_fetcher : self.xia_data, self.dmpl_data
        """

        n_inserted = 0
        n_already  = 0

        for md, run in list_to_check:

            if not self.in_db(md, run, table):
                self.db.insert('SARS_COV_2_Analysis_v2.{}'.format(table),
                               data_fetcher(md, run),
                               verbose=False)
                n_inserted += 1
            else:
                n_already += 1

        print('> {}'.format(table))
        print('inserted:       {}'.format(n_inserted))
        print('already in db:  {}'.format(n_already))

        return


    def update_xia(self):
        self._update('Data_Reduction',
                     self.fetch_xia_successes(),
                     self.xia_data)
        return


    def update_dimpling(self):
        self._update('Refinement',
                     self.fetch_dmpl_successes(),
                     self.dmpl_data)
        return


if __name__ == '__main__':
    dbd = DBDaemon.load_config('../configs/test.yaml')
    dbd.update_xia()
    #dbd.update_dimpling()


