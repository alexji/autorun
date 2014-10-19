import sys
import os
import subprocess
from optparse import OptionParser
import glob

from utils import *

########################################################
# Flexible submission script for subfind jobs

myemail="alexji@mit.edu"
scriptpath="/home/alexji/autorun"
gadgetpath="/home/alexji/P-Gadget3"

def submit_one_job(outpath,jobname,snap,levelmax,pmgrid,options):
    subfindprog = gadgetpath+"/P-Gadget3sub"
    if options.hsml: subfindprog += 'hsml'
    subfindprog += '_'+str(pmgrid)
    print subfindprog
    assert os.path.exists(subfindprog), 'Required subfind is not compiled'
    subfindcfg  = scriptpath+"/gadget_files/paramsub_"+levelmax+".txt"
    assert os.path.exists(subfindcfg), 'Required subfind parameter file does not exist'

    f = open(outpath+'/'+jobname+'.sbatch','w')
    f.write("#!/bin/sh\n")
    f.write("#SBATCH -J "+jobname+"\n") #jobname
    f.write("#SBATCH -o subfind.o \n") #jobname
    f.write("#SBATCH -e subfind.e \n") #jobname
    if options.regnodes:
        f.write("#SBATCH -p RegNodes\n") #partition
        CORES_PER_NODE=8
    elif options.amd64:
        f.write("#SBATCH -p AMD64\n") #partition
        CORES_PER_NODE=64
    else:
        f.write("#SBATCH -p HyperNodes\n") #partition
        CORES_PER_NODE=24
    nproc = str(int(options.nnodes)*CORES_PER_NODE)
    f.write("#SBATCH -N "+str(options.nnodes)+"\n") #minimum number of nodes
    f.write("#SBATCH -n "+str(nproc)+"\n") #minimum number of jobs
    f.write("#SBATCH -t "+options.time+"\n") #time
    #f.write("#SBATCH --mem="+options.mem+"\n") #memory
    #f.write("#SBATCH --ntasks-per-node="+"\n")
    f.write("#SBATCH --exclusive\n")
    f.write("\n")

    f.write('echo "tasks per node: $SLURM_TASKS_PER_NODE"\n')
    f.write("cd "+outpath+"\n")
    f.write("cp "+subfindprog+" . \n")
    f.write("cp "+subfindcfg+" . \n")
    f.write('echo "start time" `/bin/date`\n')
    f.write(". /opt/torque/etc/openmpi-setup.sh\n")
    f.write("mpirun -np "+nproc+" "+os.path.basename(subfindprog)+" "+
            os.path.basename(subfindcfg)+" 3 "+snap+"\n")
    f.write('echo "stop time" `/bin/date`\n')
    f.close()

    if not options.checkflag:
        subprocess.call(';'.join(["cd "+outpath,
                                  "sbatch "+jobname+".sbatch"]),
                        shell=True)

def obtain_snaplist(outpath,options):
    def subfind_snap_exists(outpath,snapstr):
        numsnaps = sum(1 for line in open(outpath+'/ExpansionList'))
        groups_present = os.path.exists(outpath+'/outputs/groups_'+snapstr+'/group_tab_'+snapstr+'.0')
        subhalos_present = os.path.exists(outpath+'/outputs/groups_'+snapstr+'/subhalo_tab_'+snapstr+'.0')
        return groups_present and subhalos_present
    def currently_running_subfind(outpath,snapstr,currentjobs):
        shortname = get_short_name(os.path.basename(os.path.normpath(outpath)))
        jobname = 'SUBF_'+shortname+'_'+str(options.snapnum).zfill(3)
        return jobname in currentjobs
    currentjobs = get_currently_running_jobs()
    if options.snapnum==-1:
        snaplist = []
        for filename in os.listdir(outpath+'/outputs'):
            if (filename[0:8] == "snapdir_"):
                snap = filename[8:11]
                if (not subfind_snap_exists(outpath,snap) and 
                    not currently_running_subfind(outpath,snap,currentjobs)):
                    snaplist.append(int(snap))
        return snaplist
    else:
        snap = str(options.snapnum).zfill(3)
        if (not subfind_snap_exists(outpath,snap) and 
            not currently_running_subfind(outpath,snap,currentjobs)):
            return [snap]
        else:
            #print "  SKIP: Snap "+snap+" in "+outpath+" (running/already run/snap not done)"
            return None

def determine_pmgrid(levelmax,options):
    ## I put this here so there's some flexiblity in how to determine pmgrid
    if options.pmgrid != -1: return options.pmgrid
    if (int(levelmax)==14): return str(512)
    return str(512)

def submit_jobs(outpath,options,jobnum):
    # No need to check for gadget: already done by find_halo_paths
    # Get snaplist
    if options.lastsnapflag:
        options.snapnum = sum(1 for line in open(outpath+'/ExpansionList')) - 1
    foldername=os.path.basename(os.path.normpath(outpath))
    snaplist = obtain_snaplist(outpath,options)
    if snaplist == None: #all running/already run
        return jobnum

    # Submit jobs
    for snap in snaplist:
        if jobnum >= options.numjobs:
            if (options.checkflag):
                print "Will submit "+str(jobnum)+" jobs (use -n NUMJOBS option to increase max number)"
                sys.exit()
            else:
                print "Submitted "+str(jobnum)+" jobs (use -n NUMJOBS option to increase max number)"
                sys.exit("Ending script (reached job limit)")
        snap = str(snap).zfill(3)
        if (options.lxparam==-1):
            ictype,levelmax,nrvir = get_zoom_params(outpath)
            levelmax = str(levelmax)
        else:
            levelmax = str(options.lxparam)
        pmgrid = determine_pmgrid(levelmax,options)
        shortname = get_short_name(foldername)
        jobname = 'SUBF_'+shortname+'_'+snap
        if (options.checkflag):
            print "  NOW: Will run: "+jobname
        else:
            print "  NOW: Submitting "+jobname
        submit_one_job(outpath,jobname,snap,levelmax,pmgrid,options)
        jobnum += 1
    return jobnum

if __name__=="__main__":
    parser = get_default_parser()
    parser.add_option("-s","--snap", 
                      action="store",type="int",dest="snapnum",default=-1,
                      help="specific snap number")
    parser.add_option("--lastsnap",
                      action="store_true",dest="lastsnapflag",default=False,
                      help="Run subfind on the last snap (overrides -s option)")
    parser.add_option("-n","--num-jobs",
                      action="store",type="int",dest="numjobs",default=1,
                      help="number of jobs to submit (one per snapshot)")
    parser.add_option("-N","--nnodes",
                      action="store",type="string",dest="nnodes",default="1",
                      help="argument to sbatch --nnodes (-N) (default 1)")
    parser.add_option("-t","--time",
                      action="store",type="string",dest="time",default="infinite",
                      help="argument to sbatch --time (-t) (default infinite)")
    parser.add_option("--lxparam",
                      action="store",type="int",dest="lxparam",default=-1,
                      help="Set lxparam when folder name is not default format (also determines pmgrid)")
    parser.add_option("--pmgrid",
                      action="store",type="int",dest="pmgrid",default=-1,
                      help="Set pmgrid")
    parser.add_option("--hsml",
                      action="store_true",dest="hsml",default=False,
                      help="use sorted HSML")

    (options,args) = parser.parse_args()
    if (options.autoflag):
        # Generate list of paths to halos that have not been run yet
        # For each path in the pathlist, submit a job
        if options.oldhalos: 
            halopathlist = find_halo_paths(options.lx,options.nv,
                                           basepath="/bigbang/data/AnnaGroup/caterpillar/halos/oldhalos",checkallexist=True,verbose=True,hdf5=False)
                                           #require_sorted=True)
        elif options.badics:
            halopathlist = find_halo_paths(options.lx,options.nv,
                                           basepath="/bigbang/data/AnnaGroup/caterpillar/halos/extremely_large_ics",checkallexist=True)
                                           #require_sorted=True)
        else:
            halopathlist = find_halo_paths(options.lx,options.nv,verbose=False)
                                           #require_sorted=True)
        jobnum = 0
        for outpath in halopathlist:
            jobnum = submit_jobs(outpath,options,jobnum)
    else:
        if (len(args) != 1):
            sys.exit("ERROR: must pass in valid data directory as the first argument")
        outpath = args[0]
        if (not os.path.exists(outpath)):
            sys.exit("ERROR: "+outpath+" does not exist (use -a option to automatically detect folders that need subfind run)")
        submit_jobs(outpath,options,0)
