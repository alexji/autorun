import os,sys,subprocess,glob
from optparse import OptionParser
from utils import *

myemail="alexji@mit.edu"
scriptpath="/home/alexji/autorun"
progpath="/home/alexji/rockstar/examples/calc_potentials_binary.py"

def checkdone_unbind(hpath,snap):
    indexpath = hpath+'/halos/halos_'+str(snap)+'/iterboundindex.csv'
    idspath   = hpath+'/halos/halos_'+str(snap)+'/iterboundparts.dat'
    rockstar_exists  = check_rockstar_exists(hpath,snap)
    return (os.path.exists(indexpath) and os.path.exists(idspath) and rockstar_exists)
def jobnamefn(hpath):
    foldername = get_foldername(hpath)
    return 'UB_'+get_short_name(foldername)

def submit_one_job(hpath,snap,options):
    halopath = hpath+'/halos/halos_'+str(snap)
    with open(halopath+'/unbind.sbatch','w') as f:
        f.write("#!/bin/bash\n")
        f.write("#SBATCH -J "+jobnamefn(hpath)+"\n")
        f.write("#SBATCH -o unbind.o\n")
        f.write("#SBATCH -e unbind.e\n")
        if options.regnodes: #partition
            f.write("#SBATCH -p RegNodes\n")
        elif options.amd64:
            f.write("#SBATCH -p AMD64\n")
        else:
            f.write("#SBATCH -p HyperNodes\n")
        f.write('echo "start time" `/bin/date`\n')
        functioncall = 'python '+progpath+' '+hpath+' '+str(snap)
        f.write(functioncall+'\n')
        f.write('chmod g+rwx iterboundindex.csv\n')
        f.write('chmod g+rwx iterboundparts.dat\n')
        f.write("chgrp annaproj iterboundindex.csv\n")
        f.write("chgrp annaproj iterboundparts.dat\n")
        f.write('echo "stop time" `/bin/date`\n')
    subprocess.call(';'.join(["cd "+halopath,
                              "sbatch unbind.sbatch",
                              "cd "+os.getcwd()]),
                    shell=True)
    return True

if __name__=="__main__":
    parser = get_default_parser()
    parser.add_option("-f","--force", 
                      action="store_true",dest="forceflag",default=False,
                      help="force this to rerun even if already run (as determined by the existence of a halos/ folder). Be careful with the --auto option! It's better to delete the relevant files if you want to redo something with --auto")
    parser.add_option("-s","--snap",
                      action="store",type="int",dest="snap",default=255,
                      help="Which snap to run unbinding on (default 255)")
    parser.add_option("-n","--num-jobs",
                      action="store",type="int",dest="numjobs",default=1,
                      help="number of jobs to submit (one per halo)")
    (options,args) = parser.parse_args()
    
    snap = options.snap
    mydonefn = lambda hpath: checkdone_unbind(hpath,snap)
    if (options.autoflag):
        halopathlist = find_halo_paths(options.lx,options.nv,verbose=True)
        print "Total number of halo paths: ",len(halopathlist)
        halopathlist = filter_halo_paths(halopathlist,jobnamefn,mydonefn,options.forceflag)

        print "Number of halos that need to be run (not including currently running):",len(halopathlist)
        print [os.path.basename(os.path.normpath(hpath)) for hpath in halopathlist]
        jobnum = 0
        for hpath in halopathlist:
            success = submit_one_job(hpath,snap,options)
            if success: jobnum += 1
            if jobnum >= options.numjobs:
                if (options.checkflag):
                    print "Will submit "+str(jobnum)+" jobs (use -n NUMJOBS option to increase max number)"
                    sys.exit()
                else:
                    print "Submitted "+str(jobnum)+" jobs (use -n NUMJOBS option to increase max number)"
                    sys.exit("Ending script because reached job limit.")
    else:
        raise NotImplementedError
