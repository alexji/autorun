import sys
import os
import subprocess
import glob
from optparse import OptionParser
from utils import *

########################################################
# Flexible submission script for rockstar jobs

myemail="alexji@mit.edu"
scriptpath="/home/alexji/autorun"
rockstarpath="/home/alexji/rockstar"
#rockstarpath="/home/alexji/Rockstar-Galaxies"
#rockstarpath="/home/alexji/Rockstar-0.99.9-RC3"
mergertreepath="/home/alexji/consistent_trees-0.99.9.2"

def _find_force_res(outpath):
    fname = outpath+'/param.txt-usedvalues'
    if not os.path.exists(fname): raise IOError("Could not find file "+fname)
    forceres=-1
    f = open(fname,'r')
    for line in f:
        s = line.split()
        if s[0]=="SofteningHaloMaxPhys":
            forceres = float(s[1])
            break
    f.close()
    if forceres==-1: raise IOError("Could not find force resolution")
    return forceres
def find_force_res(outpath):
    try:
        forceres = _find_force_res(outpath)
    except IOError as e:
        print "------WARNING------"
        print e
        ictype,lx,nv = get_zoom_params(outpath)
        #forceres = 100./2.^lx/40.
        forceres = 100./2.^lx/80.
    return forceres

def write_slurm_submission_script(outpath,jobname,rockstarcfg,options):
    label="bound"

    startsnap = str(options.startsnap)
    f = open(outpath+'/'+jobname+'.sbatch','w')

    f.write("#!/bin/sh\n")
    f.write("#SBATCH -J "+jobname+"\n")
    f.write("#SBATCH -o rockstar.o\n")
    f.write("#SBATCH -e rockstar.e\n")
    if options.regnodes: #partition
        f.write("#SBATCH -p RegNodes\n")
    elif options.amd64:
        f.write("#SBATCH -p AMD64\n")
    elif options.regshort:
        f.write("#SBATCH -p RegShort\n")
        if options.nnodes > 1:
            f.write("#SBATCH -t 3-00:00:00\n")
        else:
            f.write("#SBATCH -t 7-00:00:00\n")
    elif options.hypershort:
        f.write("#SBATCH -p HyperShort\n")
        if options.nnodes > 1:
            f.write("#SBATCH -t 3-00:00:00\n")
        else:
            f.write("#SBATCH -t 7-00:00:00\n")
    else:
        f.write("#SBATCH -p HyperNodes\n")
    f.write("#SBATCH -N "+options.nnodes+"\n") #minimum number of nodes
    f.write("\n")
    f.write("LABEL="+label+"\n")
    f.write("STARTSNAP="+startsnap+"\n")
    f.write("NUMSNAPS=`cat "+outpath+"/ExpansionList | wc -l`\n")
    f.write("NUMBLOCKS=`grep NumFilesPerSnapshot "+outpath+"/param.txt | awk '{print $2}'`\n")
    f.write("BOXSIZE=`grep BoxSize "+outpath+"/param.txt | awk '{print $2}'`\n")
    f.write("OUTDIR=halos_${LABEL}\n")
    f.write("HPATH="+outpath+"\n")
    f.write("echo \"numsnaps: $NUMSNAPS\"\n")
    f.write("echo \"numblocks: $NUMBLOCKS\"\n")
    f.write("echo \"boxsize: $BOXSIZE\"\n")
    f.write("echo \"tasks per node: $SLURM_TASKS_PER_NODE\"\n")
    f.write("echo \"submit directory: $SLURM_SUBMIT_DIR\"\n")
    f.write("echo \"start time\" `/bin/date`\n")
    f.write("cd ${HPATH}\n")
    if options.forceflag:
        f.write("if [ -d ${HPATH}/${OUTDIR} ]; then \n")
        f.write("    rm -rf ${HPATH}/${OUTDIR}\n")
        f.write("fi\n\n")
    else:
        f.write("#if [ -d ${HPATH}/${OUTDIR} ]; then \n")
        f.write("#    rm -rf ${HPATH}/${OUTDIR}\n")
        f.write("#fi\n")

    f.write("mkdir -p ${OUTDIR}\n")
    f.write("chmod -R g+rwx ${OUTDIR}\n")
    f.write("chgrp -R annaproj ${OUTDIR}\n")
    f.write("cd ${OUTDIR}\n")
    f.write("# auto-generating rockstar cfg\n")
    f.write("cat > ${HPATH}/${OUTDIR}/rockstar_autocfg_${LABEL}.cfg <<EOF\n")
    f.write("INBASE  = ${HPATH}/outputs\n")
    f.write("OUTBASE = ${HPATH}/${OUTDIR}\n")
    f.write("NUM_BLOCKS = ${NUMBLOCKS}\n")
    f.write("NUM_WRITERS = "+options.numwriters+"\n")
    f.write("NUM_SNAPS=${NUMSNAPS}\n")
    f.write("STARTING_SNAP=${STARTSNAP}\n")
    f.write("OUTPUT_FORMAT=\"BOTHBIN\"\n")
    f.write("PARALLEL_IO = 1\n")
    f.write("FORK_READERS_FROM_WRITERS = 1\n")
    forceres = find_force_res(outpath)
    f.write("FORCE_RES = "+str(forceres)+"\n")
    f.write("FILE_FORMAT = \"AREPO\"\n")
    f.write("FILENAME = snapdir_<snap>/snap_<snap>.<block>.hdf5\n")
    f.write("AREPO_LENGTH_CONVERSION = 1.0\n")
    f.write("MASS_DEFINITION=\"vir\"\n")
    f.write("DELETE_BINARY_OUTPUT_AFTER_FINISHED = 1\n")
    f.write("EOF\n")
    f.write("\n")
    f.write("if [ -e auto-rockstar.cfg ]; then\n")
    f.write("rm auto-rockstar.cfg\n")
    f.write("fi\n")
    f.write("\n")
    f.write(rockstarpath+"/rockstar${LABEL} -c ${HPATH}/${OUTDIR}/rockstar_autocfg_${LABEL}.cfg &\n")
    f.write("#"+rockstarpath+"/rockstar${LABEL} -c ${HPATH}/${OUTDIR}/restart.cfg &\n")
    f.write("while [ ! -e auto-rockstar.cfg ]; do\n")
    f.write("sleep 1\n")
    f.write("done\n")
    f.write("srun -n "+options.numwriters+" "+rockstarpath+"/rockstar${LABEL} -c auto-rockstar.cfg\n")
    f.write("echo \"halo catalogue stop time\" `/bin/date`\n")
    f.write(scriptpath+"/postprocess-rockstar.sh "+rockstarpath+" ${HPATH}/${OUTDIR}/rockstar_autocfg_${LABEL}.cfg "+mergertreepath+" ${HPATH}/${OUTDIR} $NUMSNAPS $STARTSNAP $BOXSIZE\n")
    f.write("cd ${HPATH}\n")
    f.write("chgrp -R annaproj ${OUTDIR}\n")
    f.write("chmod -R g+rwx ${OUTDIR}\n")
    f.write("echo \"stop time\" `/bin/date`\n")

    f.close()
    rockstarcfg = outpath+'/halos_'+label+'/rockstar_autocfg_'+label+'.cfg'
    return rockstarcfg # name of the cfg file

def submit_one_job(outpath,options,rockstarcfg,jobnum=0):
    if jobnum >= options.numjobs:
        if (options.checkflag):
            print "Will submit "+str(jobnum)+" jobs (use -n NUMJOBS option to increase max number)"
            sys.exit()
        else:
            print "Submitted "+str(jobnum)+" jobs (use -n NUMJOBS option to increase max number)"
            sys.exit("Ending script because reached job limit.")

    foldername=os.path.basename(os.path.normpath(outpath))
    jobname='RS_'+get_short_name(foldername)

    try:
        rockstarcfg = write_slurm_submission_script(outpath,jobname,rockstarcfg,options)
    except IOError as e:
        if (not options.checkflag): raise e
        print "IO error({0}): {1}, path is ".format(e.errno,e.strerror) + outpath
        print "  Since you are just checking, the error is ignored (rockstarcfg not written)"
        rockstarcfg = "<no_cfg_file>"
    if (options.checkflag):
        print "  Will run: "+jobname+" using "+rockstarcfg+" starting from snap "+str(options.startsnap)
    else:
        print "  Submitting "+jobname+" using "+rockstarcfg+" starting from snap "+str(options.startsnap)
        subprocess.call(';'.join(["cd "+outpath,
                                  "sbatch "+jobname+".sbatch",
                                  "cd "+os.getcwd()]),
                        shell=True)
    return jobnum + 1

def get_rockstar_outbase(filename):
    if (not os.path.exists(filename)):
        sys.exit("ERROR: rockstar cfg file not found: "+filename)
    f = open(filename,'r')
    for line in f:
        splits = line.split("=")
        if (splits[0].strip()=="OUTBASE"):
            f.close()
            return splits[1].strip()
    sys.exit("ERROR: rockstar cfg does not specify OUTBASE")
    #in the future: could just use current directory, which I think is the rockstar default

if __name__=="__main__":
    parser = get_default_parser()
    parser.add_option("-f","--force", 
                      action="store_true",dest="forceflag",default=False,
                      help="force rockstar to rerun even if already run (as determined by the existence of a halos/ folder). Be careful with the --auto option! It's better to delete the relevant halos/ folders if you want to redo something with --auto")
    parser.add_option("-s","--startsnap",
                      action="store",type="int",dest="startsnap",default=0,
                      help="if -a option is specified, this gives the first snap number to begin running rockstar (default 0)")
    parser.add_option("--numwriters",
                      action="store",type="string",dest="numwriters",default="8",
                      help="rockstar cfg NUM_WRITERS (default 8)")
    parser.add_option("-N","--nnodes",
                      action="store",type="string",dest="nnodes",default="1",
                      help="argument to sbatch --nnodes (-N) (default 1)")
    parser.add_option("--autostartsnap",
                      action="store_true",dest="autostartsnap",default=False,
                      help="if specified, automatically detects the snap to start rockstar (overwrites what you put for --startsnap)")
    parser.add_option("-n","--num-jobs",
                      action="store",type="int",dest="numjobs",default=1,
                      help="number of jobs to submit (one per halo)")
    parser.add_option("--middle",
                      action="store_true",dest="middle",default=False)
    parser.add_option("--low",
                      action="store_true",dest="low",default=False)
    parser.add_option("--high",
                      action="store_true",dest="high",default=False)

    (options,args) = parser.parse_args()

    if (options.autoflag):
        if options.middle:
            print "looking in middle_mass_halos"
            halopathlist = find_halo_paths(options.lx,options.nv,verbose=True,basepath="/bigbang/data/AnnaGroup/caterpillar/halos/middle_mass_halos")
        elif options.low:
            print "looking in low_mass_halos"
            halopathlist = find_halo_paths(options.lx,options.nv,verbose=True,basepath="/bigbang/data/AnnaGroup/caterpillar/halos/low_mass_halos")
        elif options.high:
            print "looking in high_mass_halos"
            halopathlist = find_halo_paths(options.lx,options.nv,verbose=True,basepath="/bigbang/data/AnnaGroup/caterpillar/halos/high_mass_halos")
        else:
            halopathlist = find_halo_paths(options.lx,options.nv,verbose=True)
                                           #require_sorted=True)
        print "Total number of halo paths: ",len(halopathlist)
        #print [os.path.basename(os.path.normpath(outpath)) for outpath in halopathlist]
        currentjobs = get_currently_running_jobs()

        listtosubmit = list(halopathlist) #copy halopathlist
        for outpath in halopathlist:
            foldername=os.path.basename(os.path.normpath(outpath))
            shortjobname = get_short_name(foldername)
            foundjob = False
            # Look through current jobs, remove currently running jobs
            for currentjob in currentjobs:
                if 'RS_'+shortjobname in currentjob:
                    print "RUNNING: "+foldername+" is running on job "+currentjob
                    foundjob = True; listtosubmit.remove(outpath); break
            totalnumsnaps = sum(1 for line in open(outpath+'/ExpansionList'))
            numsnaps = 0
            for snap in xrange(totalnumsnaps):
                snapstr = str(snap)
                if check_rockstar_exists(outpath,snap):
                    numsnaps += 1
            if numsnaps == totalnumsnaps and not options.forceflag:
                print "DONE Rockstar completed for "+os.path.basename(os.path.normpath(outpath))+", "+str(numsnaps)+" snaps"
                listtosubmit.remove(outpath)
        halopathlist = listtosubmit

        print "Number of halos that need rockstar run (not including currently running):",len(halopathlist)
        print [os.path.basename(os.path.normpath(outpath)) for outpath in halopathlist]
        jobnum = 0
        for outpath in halopathlist:
            jobnum = submit_one_job(outpath,options,None,jobnum=jobnum)
    else:
        if (len(args) != 1):
            sys.exit("ERROR: must pass in rockstar config as the first argument (or use -a option)")
        outpath = get_rockstar_outbase(args[0])
        if (not os.path.exists(outpath)):
            sys.exit("ERROR: "+outpath+" does not exist (use -a option to automatically detect folders that need rockstar run)")
        submit_one_job(outpath,options,args[0])
