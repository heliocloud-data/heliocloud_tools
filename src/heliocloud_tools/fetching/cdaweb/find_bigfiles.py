import gzip
import pickle
bigfiles = []
bigcount = 0
icount = 0
with gzip.open('filelist.gz','rt') as fin:
    for line in fin:
        if line.endswith(".cdf\n") or line.endswith(".nc\n"):
            lineset = line.rstrip().split()
            filesize = int(lineset[-2])/1000000000.0
            if filesize > 5:
                bigcount += 1
                bigfiles.append(lineset[-1])
                print(lineset[-1])
            icount += 1
            if icount % 50000 == 1: print("Up to file",icount)

print(f"There were {bigcount} files > 5GB")
if bigcount > 0:
    with open("bigfiles.pkl","wb") as fout:
        pickle.dump(bigfiles,fout)
    print("Stored filenames in bigfiles.pkl")
