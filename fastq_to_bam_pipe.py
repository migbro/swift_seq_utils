#!/usr/bin/env python
"""
Utility to merge fastq files into an unaligned bam for use in SwiftSeq
Usage: ./fastq_to_bam_pipe.py (-f FQ_LIST -j JAVA -p PICARD -n NOVOSORT -t THREADS -m MEMORY) [-c CONTAINER]

Options:
  -f  FQ_LIST  list of fastq files to download, convert, and merge to bam. Should be sorted r1, r2
  -j  JAVA  location of jav binary
  -p  PICARD  location of picard tools
  -n  NOVOSORT  location of novosort tool
  -t  THREADS  number of threads to use for novosort
  -m  MEMORY  amount of ram to use in GB for novosort and picard
  -c  CONTAINER  swift container with objects to download
  -h

"""


import sys
import os
import subprocess
import time
from docopt import docopt
args = docopt(__doc__)
sys.path.append('/home/ubuntu/TOOLS/Scripts/utility')
from job_manager import job_manager


def date_time():
    cur = ">" + time.strftime("%c") + '\n'
    return cur


sname = ''
bam_list = []
cmd_list = []
fq_list = open(args['FQ_LIST'], 'r')
p_mem = int(args['MEMORY'])//int(args['THREADS'])
for fn in fq_list:
    r1 = fn.rstrip('\n')
    r2 = next(fq_list)
    r2 = r2.rstrip('\n')
    root = r1.replace('_1_sequence.txt.gz', '')
    sys.stderr.write(date_time() + 'Processing fastq pair with root ' + root + '\n')
    if args['-c'] and args['CONTAINER'] is not None:
        swift_cmd = 'swift download ' + args['CONTAINER'] + ' --prefix ' + root
        check = subprocess.call(swift_cmd, shell=True)
        if check != 0:
            sys.stderr.write('Failed to download fastq file with root ' + root + '\n')
            exit(1)

    meta = root.split('_')
    READ_GROUP_NAME = root
    sname = meta[0]
    LIBRARY_NAME = meta[0]
    PLATFORM_UNIT = meta[4]
    PLATFORM = 'illumina'
    bam = os.path.basename(root) + '_unaligned.bam'
    bam_list.append(bam)
    log_file = READ_GROUP_NAME + '.convert.log'
    picard_cmd = args['JAVA'] + ' -Djava.io.tmpdir=tmp -Xmx' + str(p_mem) + 'G -jar ' + args['PICARD'] \
                 + ' FastqToSam FASTQ=' + r1 + ' FASTQ2=' + r2 + ' OUTPUT=' + bam + ' READ_GROUP_NAME=' \
                 + READ_GROUP_NAME + ' SAMPLE_NAME=' + sname + ' LIBRARY_NAME=' + LIBRARY_NAME + ' PLATFORM_UNIT=' \
                 + PLATFORM_UNIT + ' PLATFORM=' + PLATFORM
    picard_cmd += ' 2> ' + log_file + ' >> ' + log_file + '; rm ' + r1 + ' ' + r2
    cmd_list.append(picard_cmd)

sys.stderr.write(date_time() + 'Queueing jobs for conversion\n')
job_manager(cmd_list, args['THREADS'])
novo_cmd = 'mkdir tmp; ' + args['NOVOSORT'] + ' -c ' + args['THREADS'] + ' -m ' + args['MEMORY'] + 'G -n -t tmp ' \
           + ' '.join(bam_list) + ' > ' + sname + '_ualigned_merged.bam'
sys.stderr.write(date_time() + ' Merging unaligned bams with command ' + novo_cmd + '\n')
check = subprocess.call(novo_cmd, shell=True)
if check != 0:
    sys.stderr.write(date_time() + 'Novosort merge failed!\n')
    exit(1)
else:
    sys.stderr.write(date_time() + 'Merge complete, deleting individual bams\n')
    #rm_bam = 'rm ' + ' '.join(bam_list)
    #subprocess.call(rm_bam, shell=True)
