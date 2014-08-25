import numpy as np
import os
from utils import *
from optparse import OptionParser

thisprog = "/home/alexji/autorun/run-sort.py"
sortprog = "/home/alexji/autorun/sort_snapCP.py"

def submit_job(outpath,options,inode,snaplist):
    if check_is_sorted(outpath):
        if options.verbose:
            print "DONE: "+outpath
        return False
    else:
        print "RUN"+str(inode)+": "+outpath

    numsnaps = len(snaplist)
    startsnap = str(snaplist[0]); endsnap = str(snaplist[-1])
    ictype,lx,nv = get_zoom_params(outpath)
    jobname = 'SORT_'+get_short_name(get_foldername(outpath))+'_'+str(inode)
    f = open(outpath+'/sort.sbatch'+str(inode),'w')
    f.write("#!/bin/bash\n")
    f.write("#SBATCH -J "+jobname+"\n")
    f.write("#SBATCH -o sort.o"+str(inode)+"\n")
    f.write("#SBATCH -e sort.e"+str(inode)+"\n")
    if options.regnodes:
        f.write("#SBATCH -p RegNodes\n")
        CORES_PER_NODE=8
    elif options.amd64:
        f.write("#SBATCH -p AMD64\n")
        CORES_PER_NODE=64
    else:
        f.write("#SBATCH -p HyperNodes\n")
        CORES_PER_NODE=24
    f.write("#SBATCH -N 1\n")
    f.write("\n")

    f.write("cd "+outpath+"\n")
    f.write("if [ -e .DONESORTING"+str(inode)+" ]; then\n")
    f.write("    rm .DONESORTING"+str(inode)+"\n")
    f.write("fi\n\n")
    f.write("mkdir -p outputssorttmp\n")
    f.write("c=0\n")
    f.write("while [ $c -le 255 ]\n")
    f.write("do\n")
    f.write('    printf -v num "%03d" $c\n')
    f.write("    mkdir -p outputssorttmp/snapdir_${num}\n")
    f.write("    (( c++ ))\n")
    f.write("done\n")

    f.write('echo "start time" `/bin/date`\n')
    f.write('python '+sortprog+' --startsnap='+startsnap+' --endsnap='+endsnap+' '+\
                outpath+' '+str(numsnaps)+' '+str(options.numtasks)+' '+\
                str(lx)+' '+str(inode)+'\n')
    f.write('echo "stop time" `/bin/date`\n')
    f.write("python "+thisprog+" --checkjobs "+outpath+"\n")
    #postprocess if all things are done
    for i in range(int(options.nnodes)):
        f.write("if [ -e .DONESORTING"+str(i)+" ]; then\n")
    for logfile in ['balance.txt','cpu.txt','energy.txt','info.txt','parameters-usedvalues','PIDs.txt','timebin','timings.txt']:
        f.write("cp outputs/"+logfile+" outputssorttmp\n")
    if options.saveold:
        f.write("mv outputs BACKoutputs\n")
    else:
        f.write("rm -rf outputs\n")
        for i in range(int(options.nnodes)):
            f.write("rm .DONESORTING"+str(i)+"\n")
    f.write("mv outputssorttmp outputs\n")
    for i in range(int(options.nnodes)):
        f.write("fi\n")

    f.close()

    if not options.checkflag:
        subprocess.call(';'.join(["cd "+outpath,
                                  "sbatch sort.sbatch"+str(inode)]),
                        shell=True)
    return True

def auto_split_snaps(numsnaps,nnodes):
    numpernode,extra = divmod(numsnaps,nnodes)
    output = []
    lastsnap = -1
    for i in range(nnodes):
        startsnap = lastsnap+1
        lastsnap = startsnap+numpernode-1+(i<extra)
        output.append(np.arange(startsnap,lastsnap+1))
    return output

if __name__=="__main__":
    parser = OptionParser()
    parser.add_option("-a","--auto", 
                      action="store_true",dest="autoflag",default=False,
                      help="automatically search through directories and submit jobs")
    parser.add_option("-k","--check",
                      action="store_true",dest="checkflag",default=False,
                      help="check to see what jobs would be run without actually running them")
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
    parser.add_option("-n","--num-jobs",
                      action="store",type="int",dest="numjobs",default=1,
                      help="number of jobs to submit")
    parser.add_option("-N","--nnodes",
                      action="store",type="string",dest="nnodes",default="1",
                      help="how many nodes to split sorting onto (default 1)")
    parser.add_option("-j","--num-tasks",
                      action="store",dest='numtasks',type="int",default=1,
                      help="number of tasks per node (default 1)")
    parser.add_option("-s","--saveold",
                      action="store_true",dest="saveold",default=False,
                      help="save unsorted data")
    parser.add_option("-v","--verbose",
                      action="store_true",dest="verbose",default=False,
                      help="print out more info")
    parser.add_option("--checkjobs",
                      action="store_true",dest="checkjobs",default=False,
                      help="check to see if all sorting jobs are done")
    (options,args) = parser.parse_args()

    if options.checkjobs:
        currentjobs = get_currently_running_jobs()
        outpath = args[0]
        foldername=os.path.basename(os.path.normpath(outpath))
        shortjobname = get_short_name(foldername)
        numjobsrunning = 0
        for currentjob in currentjobs:
            if 'SORT_'+shortjobname in currentjob:
                numjobsrunning += 1
        if numjobsrunning>0:
            print numjobsrunning,"sorting jobs still running"
        else:
            print "No more sorting jobs running: script should clean up"
        exit()

    if (options.autoflag):
        halopathlist = find_halo_paths(options.lx,options.nv,verbose=True)
        print "Total number of halo paths: ",len(halopathlist)
        currentjobs = get_currently_running_jobs()
        listtosubmit = list(halopathlist) #copy halopathlist

        for outpath in halopathlist:
            foldername=os.path.basename(os.path.normpath(outpath))
            shortjobname = get_short_name(foldername)
            foundjob = False
            for currentjob in currentjobs:
                if 'SORT_'+shortjobname in currentjob:
                    print "RUNNING: "+foldername+" is running on job "+currentjob
                    foundjob = True; listtosubmit.remove(outpath); break
            
        n = 0
        for outpath in listtosubmit:
            numsnaps = get_numsnaps(outpath)
            snaplists = auto_split_snaps(numsnaps,int(options.nnodes))
            jobsubmitted = True
            for inode,snaplist in enumerate(snaplists):
                jobsubmitted = submit_job(outpath,options,inode,snaplist) & jobsubmitted
            if jobsubmitted: n += 1
            if n>=options.numjobs: 
                print "reached %i jobs (max jobs specified by -n)" % n
                break
    else:
        outpath = args[0]
        numsnaps = get_numsnaps(outpath)
        print "path:",outpath
        submit_job(outpath,options)
    if options.checkflag: print "(Not submitting jobs, checkflag was specified)"
