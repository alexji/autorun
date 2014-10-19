import sys
import os
import subprocess
import glob
from optparse import OptionParser
from utils import *

myemail="alexji@mit.edu"
scriptpath="/home/alexji/autorun"
rockstarpath="/home/alexji/rockstar"
#rockstarpath="/home/alexji/Rockstar-0.99.9-RC3"
mergertreepath="/home/alexji/consistent_trees-0.99.9.2"
gadgetpath="/home/alexji/P-Gadget3"

def generate_rockstar_cfg(f,outpath,jobname):
    ## Automatically generate a cfg file
    f.write("# auto-generating rockstar cfg\n")

    ## create cfg file
    filename = outpath+"/halos/rockstar_autocfg_"+jobname+".cfg"
    f.write("cat > "+filename+" <<EOF\n")
    f.write("INBASE  = "+outpath+"/outputs\n")
    f.write("OUTBASE = "+outpath+"/halos\n")
    f.write("NUM_BLOCKS = ${NUMBLOCKS}\n")
    f.write("NUM_WRITERS = 8\n")
    f.write("NUM_SNAPS=256\n")
    f.write("STARTING_SNAP=255\n")

    f.write("OUTPUT_FORMAT=\"BINARY\"\n")
    f.write("PARALLEL_IO = 1\n")
    f.write("FORK_READERS_FROM_WRITERS = 1\n")
    forceres = 100./(2.**11)/80.
    f.write("FORCE_RES = "+str(forceres)+"\n")
    f.write("FILE_FORMAT = \"AREPO\"\n") #hdf5 <=> arepo
    f.write("FILENAME = snapdir_<snap>/snap_<snap>.<block>.hdf5\n")
    f.write("AREPO_LENGTH_CONVERSION = 1.0\n") #hdf5 <=> arepo
    
    f.write("MASS_DEFINITION=\"vir\"\n")
    
    f.write("FULL_PARTICLE_BINARY = 8\n")
    f.write("DELETE_BINARY_OUTPUT_AFTER_FINISHED = 1\n")
    
    f.write("EOF\n\n")

    return filename

def write_submission_script(outpath,jobname,options):
    assert 'LX11' in outpath #assumed for forceres
    rockstarexe=rockstarpath+'/rockstar'
    f = open(outpath+'/'+jobname+'.sbatch','w')
    f.write("#!/bin/sh\n")
    f.write("#SBATCH -J "+jobname+"\n") #jobname
    f.write("#SBATCH -o halocats.o \n") #jobname
    f.write("#SBATCH -e halocats.e \n") #jobname
    if options.regnodes: #partition
        f.write("#SBATCH -p RegNodes\n")
        CORES_PER_NODE=8
    elif options.amd64:
        f.write("#SBATCH -p AMD64\n")
        CORES_PER_NODE=64
    else:
        f.write("#SBATCH -p HyperNodes\n")
        CORES_PER_NODE=24
    nproc = str(int(options.nnodes)*CORES_PER_NODE)
    f.write("#SBATCH -N "+options.nnodes+"\n")
    f.write("\n")

    subfindprog = gadgetpath+"/P-Gadget3subhsml_512"
    assert os.path.exists(subfindprog), 'Required subfind is not compiled'
    subfindcfg  = scriptpath+"/gadget_files/paramsub_11.txt"
    assert os.path.exists(subfindcfg), 'Required subfind parameter file does not exist'

    f.write('cp '+subfindprog+' .\n')
    f.write('cp '+subfindcfg+' .\n')
    f.write('echo "start time" `/bin/date`\n')
    f.write(". /opt/torque/etc/openmpi-setup.sh\n")
    f.write("mpirun -np "+nproc+" "+os.path.basename(subfindprog)+" "+
            os.path.basename(subfindcfg)+" 3 255\n")
    f.write('echo "stop time" `/bin/date`\n')

    f.write("NUMSNAPS=256\n")
    f.write("NUMBLOCKS=`grep NumFilesPerSnapshot "+outpath+"/param.txt | awk '{print $2}'`\n")
    f.write("BOXSIZE=100\n")
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
        rockstarcfg = generate_rockstar_cfg(f,outpath,jobname)
        halopath = outpath+'/halos'
    else: halopath = outpath
    f.write("if [ -e auto-rockstar.cfg ]; then\n")
    f.write("    rm auto-rockstar.cfg\n")
    f.write("fi\n\n")
    
    f.write(rockstarexe+" -c "+rockstarcfg+" &\n")
    f.write("while [ ! -e auto-rockstar.cfg ]; do\n")
    f.write("    sleep 1\n")
    f.write("done\n")
    f.write("srun -n 8 "+rockstarexe+" -c auto-rockstar.cfg\n")
    f.write('echo "halo catalogue stop time" `/bin/date`\n')
    f.write(scriptpath+"/postprocess-rockstar.sh "+rockstarpath+" "
            +rockstarcfg+" "+mergertreepath+" "+halopath+" 256 255 100\n")
    f.write("cd "+outpath+"\n")
    f.write("chgrp -R annaproj halos\n")
    f.write("chmod -R g+rwx halos\n")

    f.write('echo "stop time" `/bin/date`\n')
    f.close()
    return rockstarcfg # name of the cfg file

def submit_one_job(outpath,options):
    try:
        foldername = get_foldername(outpath)
        split = foldername.split('_')
        jobname = 'HC_'+split[0]+'_'+split[-1]
        write_submission_script(outpath,jobname,options)
        if not options.checkflag:
            subprocess.call(';'.join(["cd "+outpath,
                                      "sbatch "+jobname+".sbatch"]),
                            shell=True)
    except Exception as e:
        print e


if __name__=="__main__":
    parser = get_default_parser()
    parser.add_option("-f","--force", 
                      action="store_true",dest="forceflag",default=False,
                      help="Force rockstar and subfind to be rerun")
    parser.add_option("--no-rs",
                      action="store_true",dest="no-rs",default=False,
                      help="do not run rockstar")
    parser.add_option("--no-subf",
                      action="store_true",dest="no-subf",default=False,
                      help="do not run subfind")
    parser.add_option("--numwriters",
                      action="store",type="string",dest="numwriters",default="8",
                      help="rockstar cfg NUM_WRITERS (default 8)")
    parser.add_option("-N","--nnodes",
                      action="store",type="string",dest="nnodes",default="1",
                      help="argument to sbatch --nnodes (-N) (default 1)")
    parser.add_option("-v","--verbose",
                      action="store_true",dest="verbose",default=False,
                      help="print out all output from find_halo_paths")
    (options,args) = parser.parse_args()

    if options.autoflag:
        halopathlist = find_halo_paths(options.lx,options.nv,
                                       basepath='/bigbang/data/AnnaGroup/caterpillar/halos/low_mass_halos',
                                       verbose=options.verbose,contamsuite=True,onlychecklastsnap=True)
        halopathlist += find_halo_paths(options.lx,options.nv,
                                       basepath='/bigbang/data/AnnaGroup/caterpillar/halos/middle_mass_halos',
                                       verbose=options.verbose,contamsuite=True,onlychecklastsnap=True)
        halopathlist += find_halo_paths(options.lx,options.nv,
                                        basepath='/bigbang/data/AnnaGroup/caterpillar/halos/high_mass_halos',
                                        verbose=options.verbose,contamsuite=True,onlychecklastsnap=True)
        currentjobs = get_currently_running_jobs()

        listtosubmit = list(halopathlist) #copy halopathlist
        for outpath in halopathlist:
            foldername=os.path.basename(os.path.normpath(outpath))
            shortjobname = get_short_name(foldername)
            foundjob = False
            # Look through current jobs, remove currently running jobs
            for currentjob in currentjobs:
                if 'HC_'+shortjobname in currentjob:
                    print "RUNNING: "+foldername+" is running on job "+currentjob
                    foundjob = True; listtosubmit.remove(outpath); break
            # If already done, ignore
            if os.path.exists(outpath+'/halos/halos_255') and os.path.exists(outpath+'/outputs/groups_255') and not options.forceflag:
                print "DONE halocontam completed for "+os.path.basename(os.path.normpath(outpath))
                listtosubmit.remove(outpath)
        halopathlist = listtosubmit
        
        print "Number of halos that need halo finding (not including currently running):",len(halopathlist)
        print [os.path.basename(os.path.normpath(outpath)) for outpath in halopathlist]
        
        for outpath in halopathlist:
            submit_one_job(outpath,options)
    else:
        print "requires -a flag right now"
    if options.checkflag: print "(Not submitting jobs, checkflag was specified)"
