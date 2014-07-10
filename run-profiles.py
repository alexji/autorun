import numpy as np
import os
from utils import *
from optparse import OptionParser

profprog = "/spacebase/data/alexji/analysis/profiles/compute_all_profiles.py"

def submit_job(outpath,options):
    # Check which profiles to run
    allprofdat = ['rs-halo-profile.dat','rs-halo-profile-allpart.dat',
                  'subf-halo-profile.dat','subf-halo-profile-radius.dat']
    checkfnarr = [check_last_rockstar_exists, check_last_rockstar_exists,
                  check_last_subfind_exists, check_last_subfind_exists]
    hasstufftorun = False
    runarg = outpath
    for i,profdat in enumerate(allprofdat):
        if checkfnarr[i](outpath) and (not os.path.exists(outpath+'/'+profdat)):
            runarg += ' '+str(i)
            hasstufftorun = True
            print "RUN: "+outpath+"/"+profdat
    if not hasstufftorun:
        if options.verbose:
            print "DONE: "+outpath
        return

    jobname = get_short_name(get_foldername(outpath))
    f = open(outpath+'/profile.sbatch','w')
    f.write("#!/bin/sh\n")
    f.write("#SBATCH -J pr"+jobname+"\n")
    f.write("#SBATCH -o profile.o \n")
    f.write("#SBATCH -e profile.e \n")
    if options.regnodes:
        f.write("#SBATCH -p RegNodes\n")
        CORES_PER_NODE=8
    else:
        f.write("#SBATCH -p HyperNodes\n")
        CORES_PER_NODE=24
    f.write("#SBATCH -N 1\n")
    f.write("\n")
    
    f.write("cd "+outpath+"\n")
    f.write('echo "start time" `/bin/date`\n')
    f.write('python '+profprog+' '+runarg+'\n')
    f.write('echo "stop time" `/bin/date`\n')
    f.close()

    if not options.checkflag:
        subprocess.call(';'.join(["cd "+outpath,
                                  "sbatch profile.sbatch"]),
                        shell=True)

    

if __name__=="__main__":
    parser = OptionParser()
    parser.add_option("-a","--auto", 
                      action="store_true",dest="autoflag",default=False,
                      help="automatically search through directories for jobs to submit")
    parser.add_option("-k","--check",
                      action="store_true",dest="checkflag",default=False,
                      help="check to see what jobs would be run without actually running them")
    parser.add_option('-f',action='store_true',dest='forceflag',default=False,
                      help="forced to recompute potentials if already calculated")
    parser.add_option("-n","--num-jobs",
                      action="store",type="int",dest="numjobs",default=1,
                      help="number of jobs to submit (one per halo)")
    parser.add_option("--lx",
                      action="store",type="string",default="11",
                      help="comma separated list of LX values (default 11)")
    parser.add_option("--nv",
                      action="store",type="string",default="4",
                      help="comma separated list of NV values (default 4)")
    parser.add_option("--RegNodes",
                      action="store_true",dest="regnodes",default=False,
                      help="submit to RegNodes instead of HyperNodes")
    parser.add_option("-v","--verbose",
                      action="store_true",dest="verbose",default=False,
                      help="print out more info")
    options,args = parser.parse_args()

    if (options.autoflag):
        halopathlist = find_halo_paths(options.lx,options.nv,verbose=False)
        n = 0
        for outpath in halopathlist:
            submit_job(outpath,options)
            n += 1
            if n>=options.numjobs: 
                print "reached %i jobs (max jobs specified by -n)" % n
                break
    else:
        outpath = args[0]
        print "path:",outpath
        submit_job(outpath,options)

    if options.checkflag: print "(Not submitting jobs, checkflag was specified)"
