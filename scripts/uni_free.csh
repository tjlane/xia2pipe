#!/bin/csh -f

set mtzin  = $1
set mtzout = $2
set inputpdb = /asap3/petra3/gpfs/p11/2020/data/11009999/shared/mpro_references/input.pdb

# you will need realpath for the following
set self = `realpath $0`
set baseplace = `dirname $self`

set freefile = /asap3/petra3/gpfs/p11/2020/data/11009999/shared/mpro_references/free_cor.mtz
set place = $PWD

#echo $place

# pointless to place all datasets to common origin
pointless hklref $freefile hklin ${mtzin} hklout /tmp/$$_0.mtz

# uniqueify script
cp $freefile /tmp
cd /tmp
mtzinfo $$_0.mtz > $$.mtzinfo
set LABELS = `grep LABELS $$.mtzinfo`
foreach x ( $LABELS )
   if ( $x == FreeR_flag ) then
      mtzutils hklin $$_0.mtz hklout $$_01.mtz <<+
         exclude FreeR_flag
+
   
      mv $$_01.mtz $$_0.mtz
      break
   endif 
end
set XDATA = `grep XDATA $$.mtzinfo`
unique HKLOUT $$_1.mtz <<+
CELL $XDATA[2] $XDATA[3] $XDATA[4] $XDATA[5] $XDATA[6] $XDATA[7] SYMMETRY $XDATA[10]
LABOUT F=FUNI SIGF=SIGFUNI
RESOLUTION $XDATA[9]
SYMM C121
+
cad HKLIN1 $$_0.mtz HKLIN2 $$_1.mtz HKLIN3 $freefile HKLOUT $$_2.mtz <<+
LABIN FILE 1  ALLIN
LABIN FILE 2  ALLIN
LABIN FILE 3 E1 = FreeR_flag
+
freerflag HKLIN $$_2.mtz HKLOUT $$_3.mtz <<+
COMPLETE FREE=FreeR_flag
+
mtzutils hklin $$_3.mtz hklout ${place}/${mtzout} <<+
EXCLUDE FUNI SIGFUNI
SYMM C121
+
cd $place

if ( -e XYZOUT ) /bin/rm XYZOUT
