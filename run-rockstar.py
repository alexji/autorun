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
#rockstarpath="/home/alexji/rockstar"
#rockstarpath="/home/alexji/Rockstar-Galaxies"
rockstarpath="/home/alexji/Rockstar-0.99.9-RC3"
mergertreepath="/home/alexji/consistent_trees-0.99.9.2"

#global_nrvirlist=[4]
#global_levellist=[12]
global_ictype="BB"

def generate_rockstar_cfg(f,outpath,jobname,startsnap,options):
    ## Automatically generate a cfg file
    f.write("# auto-generating rockstar cfg\n")

    ## create cfg file
    filename = outpath+"/halos/rockstar_autocfg_"+jobname+".cfg"
    f.write("cat > "+filename+" <<EOF\n")
    f.write("INBASE  = "+outpath+"/outputs\n")
    f.write("OUTBASE = "+outpath+"/halos\n")
    f.write("NUM_BLOCKS = ${NUMBLOCKS}\n")
    f.write("NUM_WRITERS = "+options.numwriters+"\n")
    f.write("NUM_SNAPS=${NUMSNAPS}\n")
    f.write("STARTING_SNAP="+startsnap+"\n")

    f.write("OUTPUT_FORMAT=\"BINARY\"\n")
    f.write("PARALLEL_IO = 1\n")
    f.write("FORK_READERS_FROM_WRITERS = 1\n")
    if options.oldhalos:
        f.write("FILE_FORMAT = \"GADGET2\"\n")
        f.write("FILENAME = snapdir_<snap>/snap_<snap>.<block>\n")
    else:
        f.write("FILE_FORMAT = \"AREPO\"\n") #hdf5 <=> arepo
        f.write("FILENAME = snapdir_<snap>/snap_<snap>.<block>.hdf5\n")
        f.write("AREPO_LENGTH_CONVERSION = 1.0\n") #hdf5 <=> arepo

    f.write("MASS_DEFINITION=\"vir\"\n")
    #if (options.allhaloparticlesflag):
    #    f.write("FULL_PARTICLE_CHUNKS = ${NCORES}\n")
    #    f.write("OUTPUT_ALL_HALO_PARTICLES_ALEX = 1\n")

    f.write("FULL_PARTICLE_BINARY = "+options.numwriters+"\n")
    f.write("DELETE_BINARY_OUTPUT_AFTER_FINISHED = 1\n")
    
    f.write("EOF\n\n")

    return filename

def write_slurm_submission_script(outpath,jobname,rockstarcfg,options):
    rockstarexe = rockstarpath+"/rockstar"
    if options.multimassflag:
        rockstarexe += "-galaxies"
    if not os.path.exists(rockstarexe):
        sys.exit("ERROR: "+rockstarexe+" does not exist (perhaps you need to add the --multimass flag?)")
    startsnap = str(options.startsnap)
    f = open(outpath+'/'+jobname+'.sbatch','w')
    f.write("#!/bin/sh\n")
    f.write("#SBATCH -J "+jobname+"\n") #jobname
    f.write("#SBATCH -o rockstar.o \n") #jobname
    f.write("#SBATCH -e rockstar.e \n") #jobname
    if options.regnodes: #partition
        f.write("#SBATCH -p RegNodes\n")
    elif options.amd64:
        f.write("#SBATCH -p AMD64\n")
    else:
        f.write("#SBATCH -p HyperNodes\n")
    f.write("#SBATCH -N "+options.nnodes+"\n") #minimum number of nodes
    f.write("#SBATCH -t "+options.time+"\n") #time
    #f.write("#SBATCH --mem="+options.mem+"\n") #memory
    #f.write("#SBATCH --ntasks-per-node="+"\n")
    f.write("#SBATCH --exclusive\n")

    f.write("\n")
    f.write("#Rockstar path: "+rockstarpath+"\n")
    f.write("NUMSNAPS=`cat "+outpath+"/ExpansionList | wc -l`\n")
    f.write("NUMBLOCKS=`grep NumFilesPerSnapshot "+outpath+"/param.txt | awk '{print $2}'`\n")
    f.write("BOXSIZE=`grep BoxSize "+outpath+"/param.txt | awk '{print $2}'`\n")
    f.write('echo "numsnaps: $NUMSNAPS"\n')
    f.write('echo "numblocks: $NUMBLOCKS"\n')
    f.write('echo "boxsize: $BOXSIZE"\n')
    f.write('echo "tasks per node: $SLURM_TASKS_PER_NODE"\n')
    f.write('echo "submit directory: $SLURM_SUBMIT_DIR"\n')
    f.write('echo "start time" `/bin/date`\n')

    f.write("cd "+outpath+"\n")

    if options.autoflag:
        if options.forceflag:
            f.write("if [ -d halos ]; then \n")
            f.write("    rm -rf halos\n")
            f.write("fi\n\n")
        f.write("mkdir -p halos\n")
        f.write('chmod -R g+rwx halos\n')
        f.write("chgrp -R annaproj halos\n")
        f.write("cd halos\n")
        rockstarcfg = generate_rockstar_cfg(f,outpath,jobname,startsnap,options)
        halopath = outpath+'/halos'
    else: halopath = outpath

    f.write("if [ -e auto-rockstar.cfg ]; then\n")
    f.write("    rm auto-rockstar.cfg\n")
    f.write("fi\n\n")
    
    f.write(rockstarexe+" -c "+rockstarcfg+" &\n")
    f.write("while [ ! -e auto-rockstar.cfg ]; do\n")
    f.write("    sleep 1\n")
    f.write("done\n")

    #f.write(". /opt/torque/etc/openmpi-setup.sh\n")
    #f.write("mpirun "+rockstarexe+" -c auto-rockstar\n")
    #f.write("mpirun -np "+str(options.numwriters)+" "+rockstarexe+" -c auto-rockstar\n")
    f.write("srun -n "+str(options.numwriters)+" "+rockstarexe+" -c auto-rockstar.cfg\n")

    f.write('echo "halo catalogue stop time" `/bin/date`\n')
    ## postprocess rockstar: run merger tree, create folders
    f.write(scriptpath+"/postprocess-rockstar.sh "+rockstarpath+" "
            +rockstarcfg+" "+mergertreepath+" "+halopath+" $NUMSNAPS "+startsnap+" $BOXSIZE\n")
    f.write("cd "+outpath+"\n")
    f.write("chgrp -R annaproj halos\n")
    f.write("chmod -R g+rwx halos\n")

    f.write('echo "stop time" `/bin/date`\n')
    f.close()
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
    parser.add_option("-t","--time",
                      action="store",type="string",dest="time",default="infinite",
                      help="argument to sbatch --time (-t) (default infinite)")
    parser.add_option("--autostartsnap",
                      action="store_true",dest="autostartsnap",default=False,
                      help="if specified, automatically detects the snap to start rockstar (overwrites what you put for --startsnap)")
    parser.add_option("-n","--num-jobs",
                      action="store",type="int",dest="numjobs",default=1,
                      help="number of jobs to submit (one per halo)")
    parser.add_option("--multimass",
                      action="store_true",dest="multimassflag",default=False,
                      help="flag to set multimass version (you still have to manually set the rockstar path in the source code)")
    #parser.add_option("--allhaloparticles",
    #                  action="store_true",dest="allhaloparticlesflag",default=False,
    #                  help="flag to output every particle for every halo (having more cores should make tihs faster)")

    (options,args) = parser.parse_args()

    if (options.autoflag):
        if options.oldhalos:
            print "looking in oldhalos"
            halopathlist = find_halo_paths(options.lx,options.nv,basepath="/bigbang/data/AnnaGroup/caterpillar/halos/oldhalos",hdf5=False,
                                           require_sorted=True)
        elif options.badics:
            print "looking in extremely_large_ics"
            halopathlist = find_halo_paths(options.lx,options.nv,verbose=False,basepath="/bigbang/data/AnnaGroup/caterpillar/halos/extremely_large_ics",
                                           require_sorted=True)
        else:
            halopathlist = find_halo_paths(options.lx,options.nv,verbose=False,
                                           require_sorted=True)
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
                if os.path.exists(outpath+'/halos/halos_'+snapstr+'/halos_'+snapstr+'.0.fullbin'):
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
