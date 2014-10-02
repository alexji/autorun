import numpy as np
import os
from utils import *
from optparse import OptionParser

def submit_job(outpath,options):
#    name = 'Nvmax'
#    prog = "/spacebase/data/alexji/analysis/redshiftzero/calcVmax.py"
    name = 'SHMF'
    prog = "/spacebase/data/alexji/analysis/redshiftzero/calcSHMF.py"

    outfile = name+'.dat'
    runarg = outpath
    batchname = name+'.sbatch'
    if os.path.exists(outpath+'/'+outfile) and (not options.forceflag):
        if options.verbose: print "DONE: "+outpath
        return False
    else:
        print "RUN: "+outpath+"/"+outfile

    jobname = get_short_name(get_foldername(outpath))
    f = open(outpath+'/'+batchname,'w')
    f.write("#!/bin/bash\n")
    f.write("#SBATCH -J "+name+jobname+"\n")
    f.write("#SBATCH -o "+name+".o \n")
    f.write("#SBATCH -e "+name+".e \n")
    if options.regnodes:
        f.write("#SBATCH -p RegNodes\n")
        CORES_PER_NODE=8
    else:
        f.write("#SBATCH -p HyperNodes\n")
        CORES_PER_NODE=24
    f.write("#SBATCH -N 1\n")
    f.write("#SBATCH --share\n")
    f.write("\n")
    
    f.write("cd "+outpath+"\n")
    f.write('echo "start time" `/bin/date`\n')
    f.write('python '+prog+' '+runarg+'\n')
    f.write('echo "stop time" `/bin/date`\n')
    f.close()

    if not options.checkflag:
        subprocess.call(';'.join(["cd "+outpath,
                                  "sbatch "+batchname]),
                        shell=True)
    return True

if __name__=="__main__":
    parser = get_default_parser()
    parser.add_option('-f',action='store_true',dest='forceflag',default=False,
                      help="forced to recompute potentials if already calculated")
    parser.add_option("-n","--num-jobs",
                      action="store",type="int",dest="numjobs",default=1,
                      help="number of jobs to submit (one per halo)")
    parser.add_option("-v","--verbose",
                      action="store_true",dest="verbose",default=False,
                      help="print out more info")
    options,args = parser.parse_args()
    
    if (options.autoflag):
        halopathlist = find_halo_paths(options.lx,options.nv,
                                       require_rockstar=True,
                                       onlychecklastsnap=True,
                                       verbose=options.verbose)
        n = 0
        for outpath in halopathlist:
            jobsubmitted = submit_job(outpath,options)
            if jobsubmitted: n += 1
            if n>=options.numjobs: 
                print "reached %i jobs (max jobs specified by -n)" % n
                break
    else:
        outpath = args[0]
        print "path:",outpath
        submit_job(outpath,options)

    if options.checkflag: print "(Not submitting jobs, checkflag was specified)"
