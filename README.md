
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

## known issues
- doing lots of small SQL queries... 

## To Do : Short-Term
- flexible partition for slurm jobs
- pass optional params to dials
- scripts to quickly submit jobs
- SQL ingestion
- all daemons in one loop

## To Do : Long-Term
- re-structure code so crystal_id is first class, not metadata (!)
- fix uni_free to make it less brittle




