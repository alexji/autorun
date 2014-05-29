#!/bin/bash

if [ $# != 7 ]; then
    echo "Error: requires 7 arguments. Recieved: $#"
    echo "Usage: ./postprocess-rockstar.sh rsdir config ctrees outdir numsnaps startsnap boxsize"
    echo "$1 $2 $3 $4 $5 $6 $7"
    exit
fi

rsdir=$1
config=$2
ctrees=$3
outdir=$4
numsnaps=$5
startsnap=$6
boxsize=$7

echo "Making Merger Tree and generating parents.list"
perl $rsdir/scripts/gen_merger_cfg.pl $config
cd $ctrees
perl do_merger_tree.pl $outdir/outputs/merger_tree.cfg

cd $outdir
## make folders
echo "Making folder structure"
c=${startsnap}
let lastsnap=$numsnaps-1
while [ $c -le $lastsnap ]
do
    #echo "Making halos_$c.."
    mkdir -p "halos_$c"
    mv halos_$c.* halos_$c
    mv out_$c.* halos_$c
    (( c++ ))
done

## generate parents.list
for ((i=${startsnap};i<=$lastsnap;++i))
do
    $rsdir/util/find_parents $outdir'/halos_'$i'/out_'$i'.list' ${boxsize} > $outdir'/halos_'$i'/parents.list'
done
