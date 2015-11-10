import os
import sys
import subprocess
from subprocess import Popen,PIPE
import pysam
import swiftclient.client
from pyspark import SparkContext

#limitmode: download 10 files, take 1000 alignments from each.
limitMode = False;

testFiles=['HG02164.chrom20.ILLUMINA.bwa.CDX.low_coverage.20120522.bam',
'HG02178.chrom20.ILLUMINA.bwa.CDX.low_coverage.20120522.bam',
'HG02156.chrom20.ILLUMINA.bwa.CDX.low_coverage.20120522.bam',
'HG02154.chrom20.ILLUMINA.bwa.CDX.low_coverage.20130415.bam',
'HG02147.chrom20.ILLUMINA.bwa.PEL.low_coverage.20120522.bam',
'HG02107.chrom20.ILLUMINA.bwa.ACB.low_coverage.20130415.bam']


#hello
def getConfig():
    config = {'user':"tobj2397",
          'key':"wpfeT429::tb",
          'tenant_name':"g2015016",
          'authurl':"http://smog.uppmax.uu.se:5000/v2.0"}
    conn = swiftclient.client.Connection(auth_version=2, **config)
    return conn

sc = SparkContext(appName="search", master=os.environ['MASTER'])

def download(filename):
    f = open('/home/ubuntu/mydebug.txt','a')
    #subprocess.call("/usr/bin/swift download GenomeData "+filename,shell=True)
    try:
        hej = subprocess.check_output("swift download GenomeData "+filename+" --os-auth-url http://smog.uppmax.uu.se:5000/v2.0 --os-username tobj2397 --os-password wpfeT429::tb --os-tenant-name g2015016",stderr=subprocess.STDOUT,shell=True)
        f.write("sucess:"+hej)
    except subprocess.CalledProcessError:
        o1,o2,o3 = sys.exc_info()
        #print('Error opening'+e.output)
        f.write("buff"+o2.output+'\n') # python will convert \n to os.linesep
    f.close()

    #swiftCon = getConfig()
    #print filename
    #obj_tuple = swiftCon.get_object('GenomeData', filename)
    #with open(filename, 'w') as my_hello:
    #    my_hello.write(obj_tuple[1])
    #swiftCon.close()

def operateOnFile(filename):
    samfile=pysam.AlignmentFile(filename,"rb")
    alignmentList =[]
    if limitMode:
        fetchedAlignments = samfile.fetch(0,100)
    else:
        fetchedAlignments = samfile.fetch()

    for alignment in fetchedAlignments:
        if abs(alignment.template_length) > 999:
            alignmentList.append(samfile.filename +"\t" + str(alignment))
    samfile.close()
    return alignmentList

def extractFromFile(filename):

    #f = open('/home/ubuntu/mydebug.txt','a')
    #try:
    #    hej = subprocess.check_output("bash /home/ubuntu/g2015016-openrc.sh",stderr=subprocess.STDOUT,shell=True)
    #    f.write("hejdu source:"+hej)
    #except subprocess.CalledProcessError:
    #    o1,o2,o3 = sys.exc_info()
        #print('Error opening'+e.output)
    #    f.write("source"+o2.output+'\n') # python will convert \n to os.linesep  $

    #f.close()
    print "start Downloads.."
    download(filename)
    download(filename+".bai")
    download(filename+".bas")
    alignmentList = operateOnFile(filename)
    print "remove Downloads.."
    os.remove(filename)
    os.remove(filename+".bai")
    os.remove(filename+".bas")
    return alignmentList

conn=getConfig()
files = conn.get_container('GenomeData')[1]
filelist=[]

for data in files:
    name = data['name']
    if name.endswith(".bam"):
        filelist.append(name)

#limitmode = only 4 files to download
if limitMode:
    filelist=filelist[:4]

print filelist

filenamesRdd = sc.parallelize(filelist,2000)

# perform "extractFromFile" on each filename
result = filenamesRdd.map(extractFromFile)
try:
    subprocess.call("hdfs dfs -rm -r genomeresult",shell=True)
    print "Old files removed"
except OSError:
    print "no previous results found"
#saves everything in the folder genomeresult
result.saveAsTextFile("genomeresult")
