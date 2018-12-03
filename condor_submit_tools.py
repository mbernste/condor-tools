############################################################
#   Build a submit file for a set of HTCondor jobs
############################################################

import os
from os.path import join
import subprocess
import re

class SubmitFileBuilder:
    def __init__(
            self, 
            executable, 
            memory, 
            disk, 
            universe="vanilla", 
            op_sys="LINUX", 
            arch="X86_64", 
            op_sys_version=None,
            blacklist=None
        ):
        """
        Args:
            executable: the location of the Condor executable
                file for each job in this cluster
            memory: memory requirement in MB
            disk: disk space requirement in MB
            universe: the HTCondor universe. See: 
                http://research.cs.wisc.edu/htcondor/manual/v7.8/2_4Road_map_Running.html
            op_sys: operating system where jobs must run
            arch: machine architecture on which jobs must run
            op_sys_version: version of the operating system on 
                which jobs must run
            blacklist: a list of machines on which the jobs
                should never be scheduled
        """
        self.universe = universe
        self.op_sys = op_sys
        self.arch = arch
        self.executable = executable
        self.memory = memory
        self.disk = disk
        self.jobs = []
        self.content = ""
        self.op_sys_version = op_sys_version
        self.blacklist = blacklist

    def add_job(
            self, 
            initial_dir, 
            log_filename="condor.log", 
            error_filename="errors.log", 
            stdout_filename="stdout.log", 
            arguments=None, 
            input_files=None, 
            output_files=None
        ):
        """
        Args:
            initial_dir: the root directory for the job where 
                input and output is written
            log_filename: name of the output file where Condor
                writes its log
            error_filename: name of the file to write stderr
            stdout_filename: name of the file to write stdout
            arguments: a list of command line arguments to feed
                to the Condor executable for this job
            input_files: a list of files that should be
                transferred to where this job is running
            output_files: a list of files that will be output 
                by this job and should be sent back to the 
                submit machine
        """
        job = {}
        job["initialdir"] = initial_dir
        if arguments:
            job["arguments"] = " ".join(arguments)
        if input_files:
            job["transfer_input_files"] = ",".join(input_files)
        if output_files:
            job["transfer_output_files"] = ",".join(output_files)
        
        print ("initial_dir = " + initial_dir)
        print ("log_filename = " + log_filename)

        job["log"] = log_filename
        job["error"] = error_filename
        job["output"] = stdout_filename
        self.jobs.append(job)

    def build_file(self):
        """
        Build the submit file
        
        Returns:
            A string containing the contents of the
            submit file
        """
        self.add_key_val("universe", self.universe)

        # bUILD REQUIREMENTS STRING
        req_str = '(TARGET.OpSys == "%s") && (TARGET.Arch =="%s")' % (self.op_sys, self.arch)
        if self.op_sys_version:
            req_str += "&& (TARGET.OpSysMajorVer == %s)" % self.op_sys_version
        if self.blacklist:
            for machine in self.blacklist:
                req_str += ' && (TARGET.machine != "%s")' % machine
        self.add_key_val("requirements", '(%s)' % req_str)
        
        #self.add_key_val("requirements", '((TARGET.OpSys == "%s") && (TARGET.Arch =="%s"))' % (self.op_sys, self.arch))
        self.add_key_val("executable", self.executable)
        self.add_key_val("request_memory", self.memory)
        self.add_key_val("request_disk", str((self.disk * 1000)))
    
        # TODO Create a method to configure this
        #self.add_key_val("periodic_release", "(JobStatus == 5) && (( CurrentTime - EnteredCurrentStatus ) > 360) && ( JobRunCount < 5) && (HoldReasonCode != 1) && ( HoldReasonCode != 12 ) && (HoldReasonCode != 21) && (HoldReasonCode != 22) && (HoldReasonCode != 34)")
        self.add_key_val(
            "periodic_release", 
            "(JobStatus == 5) && (( CurrentTime - EnteredCurrentStatus ) > 360) && ( JobRunCount < 5) && (HoldReasonCode != 1) && ( HoldReasonCode != 12 ) && (HoldReasonCode != 21) && (HoldReasonCode != 22)"
        )
        self.add_key_val("on_exit_hold", "(ExitCode != 0)")

        for job in self.jobs:
            if "transfer_input_files" in job or "transfer_output_files" in job:
                self.add_key_val("should_transfer_files", "YES")
                self.add_key_val("when_to_transfer_output", "ON_EXIT")
                break
        
        # Add job-specific submition information
        self.content += "\n\n"
        for job in self.jobs:
            self.content += "\n\n"
            for k, v in job.iteritems():
                self.add_key_val(k, v)
            self.content += "queue\n"

        return self.content

    def add_key_val(self, key, val):
        self.content += self._format_submit_line(key, val)

    def _format_submit_line(self, key, val=None):
        if val == None:
            return "%s\n" % key
        else:
            return "%s\t = %s\n" % (key, val)


def submit(submit_file):
    """
    Returns:
        Number of jobs submitted
        Cluster ID of the submitted jobs
    """
    cwd = os.getcwd()   
    print "The current working directoryt is %s" % cwd
    output = subprocess.check_output(["condor_submit", submit_file])
    match_obj = re.search(r'([0-9]+) job\(s\) submitted to cluster ([0-9]+)', output, re.DOTALL)
    return (int(match_obj.group(1).split()[-1]), match_obj.group(0).split()[-1])



