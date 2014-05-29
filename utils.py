import sys
import os
import subprocess
import glob

username = "alexji"
scriptpath = "/home/alexji/autorun"
global_ictype="BB"
#global_nrvirlist=[3,4,5,6]
#global_levellist=[11,12,13]

def get_short_name(filename):
    fileparts =  filename.split("_")
    haloid = fileparts[0]
    ictype = fileparts[1]
    levelmax = fileparts[5][2:4]
    nrvir = fileparts[7][-1]
    return haloid+ictype+"LX"+levelmax+"NV"+nrvir

def find_halo_paths(lx,nv,
                    checkallexist=False,
                    basepath="/bigbang/data/AnnaGroup/caterpillar/halos",
                    #nrvirlist=global_nrvirlist,levellist=global_levellist,
                    verbose=True):
    """
    Returns a list of paths to halos that have gadget completed/rsynced 
    with the specified nrvirlist and levellist
    """
    levellist = [int(level) for level in lx.split(',')]
    nrvirlist = [int(nrvir) for nrvir in nv.split(',')]
    
    if verbose:
        print "nrvirlist",nrvirlist
        print "levellist",levellist
    def gadget_finished(outpath,hdf5=True):
        numsnaps = sum(1 for line in open(outpath+'/ExpansionList'))
        gadgetpath = outpath+'/outputs'
        if (not os.path.exists(gadgetpath)):
            if verbose: print "Gadget not run on "+outpath
            return False
        for snap in xrange(numsnaps): # check that all snaps are there
            snapstr = str(snap).zfill(3)
            #check for existence of block 0
            snapfile = gadgetpath+"/snapdir_"+snapstr+"/snap_"+snapstr+".0"
            if hdf5:
                snapfile += ".hdf5"
            if (not os.path.isfile(snapfile)):
                if verbose: print "Snap "+snapstr+" not in "+outpath
                return False
            #check all blocks are valid
            if checkallexist:
                for snapfile in glob.glob(gadgetpath+"/snapdir_"+snapstr+'/*'):
                    if (os.path.getsize(snapfile) <= 0):
                        if verbose: print snapfile,"has no data (skipping)"#"Snap "+snapstr+" missing valid block in "+outpath
                        return False
        return True

    halopathlist = []
    haloidlist = []
    for filename in os.listdir(basepath):
        if filename[0] == "H":
            haloidlist.append(filename)
    for i,haloid in enumerate(haloidlist):
        subdirnames = basepath + "/" + haloid
        halosubdirlist = []
        try:
            for filename in os.listdir(subdirnames):
                halosubdirlist.append(filename)
                fileparts =  filename.split("_")
                levelmax = float(fileparts[5][2:4])
                nrvir = fileparts[7][-1]
                haloid = fileparts[0]
                if (int(levelmax) in levellist and int(nrvir) in nrvirlist and fileparts[1]==global_ictype):
                    outpath = basepath+"/"+haloid+"/"+filename
                    if gadget_finished(outpath):
                        halopathlist.append(outpath)
        except:
            continue
    return halopathlist

def get_currently_running_jobs(verbose=False):
    subprocess.call("squeue -h -u "+username+" -o '%j' > "+scriptpath+"/.CURRENTQUEUE",
                    shell=True)
    currentjobs = []
    f = open(scriptpath+'/.CURRENTQUEUE')
    for line in f:
        currentjobs.append(line.strip())
    f.close()
    print "Current Jobs from user "+username+": ("+str(len(currentjobs))+" jobs)"
    if verbose:
        print currentjobs
    return currentjobs
