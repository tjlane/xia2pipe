


from projbase import ProjectBase

_ALLOWED_TABLES = ['SARS_COV_2_Analysis_v2_test.Data_Reduction',
                   'SARS_COV_2_Analysis_v2_test.Refinement']


class DBDaemon:
    """
    Run through the data and update the SQL DB accordingly
    """

    def _inject_data(self, table, data_dict):

        if table not in _ALLOWED_TABLES:
            raise ValueError('{} not in list of allowed tables'.format(table))

        sql = 'INSERT INTO {} ({}) VALUES ({});'.format(table, 
                                                        data_dict.keys(), 
                                                        data_dict.values())

        db.execute(sql)

        return


    def update_xia(self):
        return

    def update_dimpling(self):
        return

