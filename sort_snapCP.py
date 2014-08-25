import numpy as np
import readsnapshots.readsnapHDF5_greg as rsg
import time, sys
import readsnapshots.hdf5lib as hdf5lib

import subprocess
from optparse import OptionParser
from multiprocessing import Pool
import functools
import os

# use this to sort caterpillar halos

#("key", "uint32"),

##########################
#SPECIFY DIRECTORIES HERE#
##########################

#snap = 0 # snap
#basein = "/bigbang/data/AnnaGroup/caterpillar/halos/H1327707/H1327707_BB_Z127_P7_LN7_LX13_O4_NV4/outputs/"
#baseout = "/bigbang/data/AnnaGroup/caterpillar/halos/H1327707/H1327707_BB_Z127_P7_LN7_LX13_O4_NV4/testoutput_sorted/"
## note - for antares runs, must use AREPO type of NTSC. Not Gadget, as on Odyssey

def rewrite(snap,basein,baseout,MASSFLAG=True):
    """
    snap: snapshot number to convert
    basein: folder containing snapshot folders of gadget output
    baseout: where to rewrite all the snapshots to
    """
   
    print "----Sorting snap %3i----" % (int(snap))
    sys.stdout.flush()

    filenamein=basein+"snapdir_"+str(0).zfill(3)+"/"+"snap_"+str(0).zfill(3)
    filenameoutb=baseout+"/snapdir_"+str(snap).zfill(3)+"/"+"snap_"+str(snap).zfill(3)

    f=hdf5lib.OpenFile(filenamein+'.0.hdf5')
    dblocks = ["ParticleIDs","Potential","Coordinates","Velocities"]
    dtypes= [str(hdf5lib.GetData(f,'PartType1/'+block_name)[0:1].dtype) for block_name in \
dblocks]
    if MASSFLAG:
        masstype = str(hdf5lib.GetData(f,'PartType5/Masses')[0:1].dtype)
    f.close()

    dtype = [("ID", dtypes[0]),
             ("POT",dtypes[1]),
             ("POS", "3"+dtypes[2]),
             ("VEL", "3"+dtypes[3])]


    if MASSFLAG:
        dtype2 = [("ID", dtypes[0]),
                  ("POT", dtypes[1]),
                  ("POS", "3"+dtypes[2]),
                  ("VEL", "3"+dtypes[3]),
                  ("MASS", masstype)]


    ########################
    #READ ORIGINAL SNAPSHOT#
    ########################
    filenamein=basein+"/snapdir_"+str(snap).zfill(3)+"/"+"snap_"+str(snap).zfill(3)
    #print "read:", filenamein
    head=rsg.snapshot_header(filenamein)
    nfiles = head.filenum #number of files its split into
    Narr = head.nall
    nper_arr = Narr/nfiles
    nlast_arr = Narr-nper_arr*(nfiles-1)
    #print nper_arr, nlast_arr, 'nper and nlast'
    head.npart=nper_arr #rewrite this part of the header
    head.sorted='yes' #Alex added this

    sortthis = False #check to see if this guy is already sorted
    for i in range(nfiles):
        if not os.path.exists(filenameoutb+'.'+str(i)+'.hdf5'):
            sortthis=True
            break
    if not sortthis: #this is already sorted, check the sorting
        allsorted = ""
        for parttype in [0,1,2,3,4,5]:
            ids = rsg.read_block(filenameoutb,"ID  ",parttype=parttype)
            if np.all(np.diff(ids)==1): allsorted += str(parttype)
        if allsorted=="012345":
            print "Snap "+str(snap)+" is already sorted"
            return
        else:
            print "ERROR: snap "+str(snap)+" is NOT sorted! ("+allsorted+" sorted)"

    try:
        for parttype in [0,1,2,3,4,5]:
            if Narr[parttype]==0:
                continue
            nper = nper_arr[parttype]; nlast = nlast_arr[parttype]; N = Narr[parttype]
            if parttype==5 and MASSFLAG==True:
                data = np.ndarray((N,),dtype=dtype2)
            else:
                data=np.ndarray((N,),dtype=dtype)
            data["ID"]  = rsg.read_block(filenamein, "ID  ",parttype=parttype)
            data["POS"] = rsg.read_block(filenamein, "POS ",parttype=parttype)
            data["VEL"] = rsg.read_block(filenamein, "VEL ",parttype=parttype)
            data["POT"] = rsg.read_block(filenamein, "POT ",parttype=parttype)
            if parttype==5 and MASSFLAG==True:
                data["MASS"] = rsg.read_block(filenamein, "MASS",parttype=parttype)
            
            ###############
            # SORT        #
            ###############
            if len(data["ID"]) !=0:
                t0=time.time()
                data=np.sort(data, order="ID")
                t1=time.time()
                print "snap %i parttype %i: sorting took (sec) = %f" % (int(snap),parttype,t1-t0)
                sys.stdout.flush()
                #print np.min(data["ID"]), np.max(data["ID"])
                #print data["ID"][0], data["ID"][-1]
    
            #######################
            #WRITE SORTED SNAPSHOT#
            #######################
            #Instead of writing all out at once, split between many files.
            for i in range(nfiles):
                filenameout=filenameoutb+"."+str(i)
                if parttype==1:
                    f=rsg.openfile(filenameout+".hdf5",mode="w")
                    if i==(nfiles-1): #special case of last file
                        head.npart=nlast_arr
                    rsg.writeheader(f, head)
                else:
                    f=rsg.openfile(filenameout+".hdf5",mode="a")
                offset = Narr[0:parttype].sum()
                if i<(nfiles-1):
                    rsg.write_block(f, "POS ", parttype, data["POS"][i*nper:nper*(i+1)])
                    rsg.write_block(f, "VEL ", parttype, data["VEL"][i*nper:nper*(i+1)])
                    rsg.write_block(f, "POT ", parttype, data["POT"][i*nper:nper*(i+1)])
                    if parttype==5 and MASSFLAG==True:
                        rsg.write_block(f, "MASS", parttype, data["MASS"][i*nper:nper*(i+1)])
                    rsg.write_block(f, "ID  ", parttype, np.arange(offset+i*nper,offset+nper*(i+1),dtype=dtypes[0]))

                else: # special case of last file
                    rsg.write_block(f, "POS ", parttype, data["POS"][i*nper:])
                    rsg.write_block(f, "VEL ", parttype, data["VEL"][i*nper:])
                    rsg.write_block(f, "POT ", parttype, data["POT"][i*nper:])
                    if parttype==5 and MASSFLAG==True:
                        rsg.write_block(f, "MASS", parttype, data["MASS"][i*nper:])
                    rsg.write_block(f, "ID  ", parttype, np.arange(offset+i*nper,offset+nper*i+nlast,dtype=dtypes[0]))
                rsg.closefile(f)
                #if i%10==0:
                    #print "write: done with file", i, 'parttype', parttype
    except MemoryError as e:
        print "Memory error ", e
        exit()
    except:
        print "Unexpected error:", sys.exc_info()[0]
        exit()


if __name__=="__main__":
    parser = OptionParser()
    parser.add_option("--startsnap", 
                      action="store",type="int",dest="startsnap",default=0,
                      help="starting snapshot")
    parser.add_option("--endsnap", 
                      action="store",type="int",dest="endsnap",default=255,
                      help="final snapshot (inclusive)")
    (options,args) = parser.parse_args()

    outpath,numsnaps,numprocs,lx,inode = args
    numsnaps = int(numsnaps)
    numprocs = int(numprocs)
    lx=int(lx)

    assert numsnaps==(options.endsnap-options.startsnap+1)

    basein = outpath+'/outputs/'
    baseout= outpath+'/outputssorttmp/'
    if lx==11: MASSFLAG=False
    else: MASSFLAG=True

    print "using",numprocs,"procs on",numsnaps,"snaps"
    print "starting with "+str(options.startsnap)+" ending with "+str(options.endsnap)

    allstart = time.time()
    pool = Pool(numprocs)
    myfunc = functools.partial(rewrite,basein=basein,baseout=baseout,
                               MASSFLAG=MASSFLAG)
    pool.map(myfunc,range(options.startsnap,options.endsnap+1))
    pool.close()
    print "done with pool: %f" % (time.time()-allstart)
    
    f = open(outpath+'/.DONESORTING'+str(inode),'w')
    f.write("\n")
    f.close()

#if __name__=="__main__":
#    import sys
#    rewrite(int(sys.argv[1]), sys.argv[2], sys.argv[3])
