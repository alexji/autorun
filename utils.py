import sys
import os
import subprocess
import glob
import h5py
from optparse import OptionParser
import readsnapshots.readsnapHDF5_greg as rs
username = "alexji"
scriptpath = "/home/alexji/autorun"
#global_ictype="BB"
#global_nrvirlist=[3,4,5,6]
#global_levellist=[11,12,13]

#caterpillar analysis module
import haloutils

def get_short_name(filename):
    fileparts =  filename.split("_")
    haloid = fileparts[0]
    ictype = fileparts[1]
    levelmax = fileparts[5][2:4]
    nrvir = fileparts[7][-1]
    return haloid+ictype+"LX"+levelmax+"NV"+nrvir

def find_halo_paths(lx,nv,
                    ictype="BB",
                    contamsuite=False,
                    require_sorted=False,
                    require_rockstar=False,
                    checkallexist=False,
                    onlychecklastsnap=False,
                    basepath="/bigbang/data/AnnaGroup/caterpillar/halos",
                    #nrvirlist=global_nrvirlist,levellist=global_levellist,
                    verbose=True,hdf5=True):
    """
    Returns a list of paths to halos that have gadget completed/rsynced 
    with the specified nrvirlist and levellist
    """
    levellist = [int(level) for level in lx.split(',')]
    nrvirlist = [int(nrvir) for nrvir in nv.split(',')]
    
    if verbose:
        print "nrvirlist",nrvirlist
        print "levellist",levellist

    return haloutils.find_halo_paths(basepath=basepath,
                                     nrvirlist=nrvirlist,levellist=levellist,
                                     ictype=ictype,verbose=verbose,hdf5=hdf5,
                                     contamsuite=contamsuite,
                                     checkallblocks=checkallexist,
                                     require_sorted=require_sorted,
                                     onlychecklastsnap=onlychecklastsnap,
                                     require_rockstar=require_rockstar)

def get_currently_running_jobs(verbose=False):
    subprocess.call("squeue -h -u "+username+" -o '%j' > "+scriptpath+"/.CURRENTQUEUE",
                    shell=True)
    currentjobs = []
    f = open(scriptpath+'/.CURRENTQUEUE')
    for line in f:
        currentjobs.append(line.strip())
    f.close()
    if verbose:
        print "Current Jobs from user "+username+": ("+str(len(currentjobs))+" jobs)"
        print currentjobs
    return currentjobs

def get_numsnaps(outpath):
    return haloutils.get_numsnaps(outpath)
def get_foldername(outpath):
    return haloutils.get_foldername(outpath)
def get_parent_hid(outpath):
    return haloutils.get_parent_hid(outpath)
def get_zoom_params(outpath):
    return haloutils.get_zoom_params(outpath)
def check_last_subfind_exists(outpath):
    return haloutils.check_last_subfind_exists(outpath)
def check_last_rockstar_exists(outpath,fullbin=True,particles=False):
    return haloutils.check_last_rockstar_exists(outpath,fullbin=fullbin,particles=particles)
def check_is_sorted(outpath,snap=0,hdf5=True):
    return haloutils.check_is_sorted(outpath,snap=snap,hdf5=hdf5)

def get_default_parser():
    parser = OptionParser()
    parser.add_option("-a","--auto",action="store_true",dest="autoflag",default=False,
                      help="automatically search through caterpillar directories")
    parser.add_option("-k","--check",
                      action="store_true",dest="checkflag",default=False,
                      help="check to see what jobs would be run without actually running them (writes submission scripts)")
    parser.add_option("--RegNodes",
                      action="store_true",dest="regnodes",default=False,
                      help="submit to RegNodes instead of HyperNodes")
    parser.add_option("--AMD64",
                      action="store_true",dest="amd64",default=False,
                      help="submit to AMD64 instead of HyperNodes")
    parser.add_option("--lx",
                      action="store",type="string",default="11",
                      help="comma separated list of LX values (default 11)")
    parser.add_option("--nv",
                      action="store",type="string",default="4",
                      help="comma separated list of NV values (default 4)")
    parser.add_option("--oldhalos",action="store_true",dest="oldhalos",default=False)
    parser.add_option("--badics",action="store_true",dest="badics",default=False)
    return parser
