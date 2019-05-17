#!/usr/bin/python

import re
import sys
from subprocess import Popen, PIPE
from optparse import OptionParser

def get_job_status_in_queue(job_id):
    """
    Example output:
    '
    -- Submitter: mammoth-1.biostat.wisc.edu : <144.92.73.108:40318> : mammoth-1.biostat.wisc.edu
    ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD               
    2478.45  mnbernstein         8/26 13:33   0+00:02:18 H  0   24.4 tblastn -db orthom
    '
    """
    p1 = Popen(["condor_q", "-nobatch", job_id], stdout=PIPE)
    output = p1.communicate()[0]
    print "The output for job %s is '%s'" % (job_id, output)
    if len(output.split("\n")) < 5 or not output.split("\n")[4].strip(): # The desired output will be on the 5th line
        #p1 = Popen(["condor_history", job_id], stdout=PIPE)
        #output = p1.communicate()[0]
        #if len(output.split("\n")) == 3 and output.split("\n")[1].split()[5].strip() == "C": 
        #    return "C"
        #else:
        #    return None
        return None

    #print output
    return output.split("\n")[4].split()[5].strip() 


def get_completed_job_ids():
    p1 = Popen(["condor_history"], stdout=PIPE)
    output = p1.communicate()[0]
    completed_jobs = []
    for output_line in output.split("\n")[1:-1]:
        if output_line.split()[5].strip() == "C":
            completed_jobs.append(output_line.split()[0])
    print "Completed jobs: %s" % str(completed_jobs)
    return completed_jobs


#def get_job_ids_OLD(cluster_id, job_status=None):
#    output = None
#    if job_status is None:
#        p1 = Popen(["condor_q", cluster_id], stdout=PIPE)
#        output = p1.communicate()[0]
#        print "The output is %s" % output
#    else:
#        p1 = Popen(["condor_q", cluster_id], stdout=PIPE)
#        p2 = Popen(["grep", job_status], stdin=p1.stdout, stdout=PIPE)
#        output = p2.communicate()[0]
#
#    job_ids = re.findall(r'^([0-9]+\.[0-9]+)', output, re.M)
#    return job_ids


def get_job_ids(cluster_id, job_status=None):
    output = None
    if job_status is None:
        p1 = Popen(["condor_q", "-nobatch", cluster_id], stdout=PIPE)
        output = p1.communicate()[0]
    else:
        p1 = Popen(["condor_q", "-nobatch", cluster_id], stdout=PIPE)
        p2 = Popen(["grep", job_status], stdin=p1.stdout, stdout=PIPE)
        output = p2.communicate()[0]

    job_ids = []
    for l in output.split("\n")[4:-3]:
        job_ids.append(l.split()[0])
    return job_ids


def get_job_indices(cluster_id, job_status=None):
    return [
        int(x.strip().split(".")[-1]) 
        for x in get_job_ids(cluster_id, job_status=job_status)
    ]


def main():
    parser = OptionParser()
    parser.add_option("-s", "--job_status", help="Filter by condor job status. Options 'R', 'H', 'I', or 'X'")
    (options, args) = parser.parse_args()

    if len(args) < 1:
        print "Usage: {options} <cluster ID>" 
    if not options.job_status is None:
        print get_job_indices(args[0], job_status=options.job_status)
    else:
         print get_job_indices(args[0])

if __name__ == "__main__":
    main()
