
"""
dials_id
crystal_id
run_id

analysis_time
folder


[get these from POINTLESS]
resolution_cc
resolution_isigma


integrater = d['_crystals']['DEFAULT']['_samples']['X1']['_sweeps'][0]['_integrater']

a/b/c/al/be/ga  integrater['_integrater']['_intgr_cell']
space_group integrater['_integrater']['_intgr_spacegroup_number']


scalr = d['_crystals']['DEFAULT']['_scaler']['_scalr_statistics']['["AUTOMATIC", "DEFAULT", "NATIVE"]']

isigi       scalr['I/sigma'][1]
meas        scalr['Total observations'][1]
cchalf      scalr['CC half'][1]
rfactor     scalr['Rmeas(I)'][1]
"""



from projbase import ProjectBase


class DBDaemon:
    """
    Run through the data and update the SQL DB accordingly
    """






