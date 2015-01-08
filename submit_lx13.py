import os,subprocess
import haloutils

if __name__=="__main__":
    coredict = {"AMD64":64,"HyperNodes":24,"RegNodes":8}
    gadgetdict = {"11":"P-Gadget3_256","12":"P-Gadget3_256",
                  "13":"P-Gadget3_512","14":"P-Gadget3_1024"}
    lx="13"
    SUBMIT=False
    
    haloidlist = [1354437,447649,581141,649861]
    partition="AMD64"
    nnodes="1"
    #partition="RegNodes"
    #nnodes="2"

    haloidlist = [1599902,1631506] #[5320,1195448]
    partition="HyperNodes"
    nnodes="6"

    #haloidlist = [95289,1195448,1354437,447649,5320,581141,649861,1599902,1631506,1130025]
    #haloidlist = [1725139,581180,94687]
    #haloidlist = [95289,1195448,1354437,447649,5320,581141,649861,1599902,1631506,1130025,1725139,94687,
    #              581180,1422331,1387186,1725272,1725372,264569,1232164]
    #haloidlist = [1631506,1292085,1354437]
    #haloidlist = [1268839]
    #haloidlist = [768257,196589]
    haloidlist = [95289,1195448,1354437,447649,5320,581141,1599902,1631506,1130025,1725139,94687,
                  581180,1422331,1387186,1725272,1725372,264569,1232164,649861,1292085,1268839,
                  768257,196589,1422331]
    haloidlist = [1268839,768257,1422331,1292085,1848355,196589]

    ncores = str(int(nnodes)*coredict[partition])
    if partition=="AMD64": bindtocore="--bind-to-core"
    elif partition=="HyperNodes": bindtocore=""
    elif partition=="RegNodes": bindtocore="--bind-to-core"

    for hid in haloidlist:
        hidstr = haloutils.hidstr(hid)
        hpaths = haloutils.get_available_hpaths(hid,checkgadget=False)
        for hpath in hpaths:
            if 'LX'+lx in hpath: break
        assert 'LX'+lx in hpath,hpath
        print hpath
        ictype,lx,nv = haloutils.get_zoom_params(hpath)
        lx = str(lx); nv = str(nv)
        if os.path.exists(hpath+'/outputs/cpu.txt'):
            #subprocess.call('tail -37 '+hpath+'/outputs/cpu.txt | head',shell=True)
            subprocess.call('tail -31 '+hpath+'/outputs/cpu.txt | head',shell=True)
        else:
            #subprocess.call('du -shc '+hpath+'/ics.*',shell=True)
            print haloutils.hidstr(hid)+" not run yet!"
            if SUBMIT:
                with open(hpath+'/runscript_gadget','w') as f:
                    f.write("#!/bin/bash \n")
                    f.write("#SBATCH -o "+hidstr+".o%j \n")
                    f.write("#SBATCH -e "+hidstr+".e%j \n")
                    f.write("#SBATCH -n "+ncores+"\n")
                    f.write("#SBATCH -N "+nnodes+"\n")
                    f.write("#SBATCH -p "+partition+"\n")
                    f.write("#SBATCH -J "+hidstr+"X"+lx[1]+"N"+nv[0]+ictype+"\n")
                    f.write("\n")
                    f.write("cd "+hpath+"\n")
                    f.write("\n")
                    f.write("mkdir -p outputs\n")
                    f.write("cp ~/autorun/gadget_files/param_"+lx+".txt ./param.txt\n")
                    f.write("cp ~/autorun/gadget_files/"+gadgetdict[lx]+" ./P-Gadget3\n")
                    f.write("cp ~/autorun/gadget_files/ExpansionList_full ./ExpansionList\n")
                    f.write("mpirun -np "+ncores+" "+bindtocore+" ./P-Gadget3 param.txt 1>OUTPUT 2>ERROR\n")
                    f.write("\n")
                    f.write("#cd "+hpath+"\n")
                    f.write("#mpirun -np "+ncores+" "+bindtocore+" ./P-Gadget3 param.txt 1 1>>OUTPUT 2>>ERROR\n")
                subprocess.call(';'.join(["cd "+hpath,"sbatch runscript_gadget"]),shell=True)
            else: pass
