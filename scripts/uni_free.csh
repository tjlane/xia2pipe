#!/bin/csh -f

set mtzin    = $1
set mtzout   = $2
set freefile = $3

echo "$0 $1 $2 $3"

# pointless to place all datasets to common origin
pointless hklref $freefile hklin ${mtzin} hklout ./$$_0.mtz

# uniqueify script
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
SYMM $XDATA[10]
+
cad HKLIN1 $$_0.mtz HKLIN2 $$_1.mtz HKLIN3 $freefile HKLOUT $$_2.mtz <<+
LABIN FILE 1  ALLIN
LABIN FILE 2  ALLIN
LABIN FILE 3 E1 = FreeR_flag
+
freerflag HKLIN $$_2.mtz HKLOUT $$_3.mtz <<+
COMPLETE FREE=FreeR_flag
+
mtzutils hklin $$_3.mtz hklout ./${mtzout} <<+
EXCLUDE FUNI SIGFUNI
SYMM $XDATA[10]
+

if ( -e XYZOUT ) /bin/rm XYZOUT
if ( -e $$_0.mtz ) /bin/rm $$_0.mtz
if ( -e $$_1.mtz ) /bin/rm $$_1.mtz
if ( -e $$_2.mtz ) /bin/rm $$_2.mtz
if ( -e $$_3.mtz ) /bin/rm $$_3.mtz
if ( -e $$.mtzinfo ) /bin/rm $$.mtzinfo

