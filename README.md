
### xia2pipe ###

TJ Lane <thomas.lane@desy.de>
First written: April 2020

-------------------------------------------------------------------------------

Some simple software for managing an automatic data proessing pipeline using
the highly automatic xia2.

Consists of 3 main parts:

* The `XiaDaemon` that finds new diffraction data and submits xia2 jobs
* The `DimplingDaemon` that finds completed xia2 jobs and refines them
* The `DBDaemon` that puts the results in an SQL database

-------------------------------------------------------------------------------

## To Do

- consolidate/generalize slurm submissions
- yaml input/init file
- all daemons in one loop




