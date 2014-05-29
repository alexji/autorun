#!/bin/bash

# This script creates symbolic links to all the relevant files
# needed to rerun the merger tree. It then reruns the merger tree and cleans up the links.

# You need to manually specify the number of writers (rockstar "blocks")

# For example:
# ./undo-postprocess-rockstar.sh /home/alexji/Rockstar-Galaxies /bigbang/data/AnnaGroup/caterpillar/halos/H268422/H268422_BE_Z127_P7_LN7_LX13_O4_NV3/halos/rockstar_autocfg_rockstar_H268422_BE_Z127_P7_LN7_LX13_O4_NV3.cfg /home/alexji/consistent_trees-0.99.9.2 /bigbang/data/AnnaGroup/caterpillar/halos/H268422/H268422_BE_Z127_P7_LN7_LX13_O4_NV3/halos 256 16

if [ $# != 6 ]; then
    echo "Error: requires 6 arguments. Recieved: $#"
    echo "Usage: ./undo-postprocess-rockstar.sh rsdir config ctrees outdir numsnaps numwriters"
    echo "$1 $2 $3 $4 $5 $6"
    exit
fi

echo "Rockstar directory: $1"
echo "Rockstar config: $2"
echo "Mergertree directory: $3"
echo "Directory to undo: $4"
echo "Num snaps: $5"
echo "Num writers: $6"

rsdir=$1
config=$2
ctrees=$3
outdir=$4
numsnaps=$5
numwriters=$6

let lastsnap=$numsnaps-1
let lastwriter=$numwriters-1

echo "Making symbolic links"
cd $outdir
##snap=0
##while [ $snap -le $lastsnap ]; do
for ((snap=0;snap<=$lastsnap;++snap)); do
    for ((writer=0;writer<=$lastwriter;++writer)); do
	#echo "halos_${snap}.${writer}.bin"
	ln -s halos_$snap/halos_$snap.$writer.bin halos_$snap.$writer.bin
    done
    ln -s halos_$snap/out_$snap.list out_$snap.list
    #(( snap++ ))
done

echo "Remaking Merger Tree"
perl $rsdir/scripts/gen_merger_cfg.pl $config
cd $ctrees
perl do_merger_tree.pl $outdir/outputs/merger_tree.cfg

echo "Removing symbolic links"
cd $outdir
for ((snap=0;snap<=$lastsnap;++snap)); do
    for ((writer=0;writer<=$lastwriter;++writer)); do
	rm halos_$snap.$writer.bin
    done
    rm out_$snap.list
done
