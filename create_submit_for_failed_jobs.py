from optparse import OptionParser
import os
from os.path import join, getsize, isfile

def main():
    usage = "" # TODO 
    parser = OptionParser(usage=usage)
    #parser.add_option("-a", "--a_descrip", action="store_true", help="This is a flat")
    #parser.add_option("-b", "--b_descrip", help="This is an argument")
    (options, args) = parser.parse_args()

    condor_root = args[0]
    submit_fname = args[1]
    
    submit_f = join(condor_root, submit_fname)
    chunks = []
    with open(submit_f, 'r') as f:
        curr_chunk = []
        for l in f:
            if l.strip() == "" and len(curr_chunk) > 0:
                chunks.append(curr_chunk)
                curr_chunk = []
            elif l.strip() == "" and len(curr_chunk) == 0:
                continue
            else:
                curr_chunk.append(l.strip())

    error_files = set()
    error_chunks = []
    for chunk in chunks[1:]:
        job_root = None
        output_fname = None
        for l in chunk:
            toks = l.split("=")
            if len(toks) < 2:
                continue
            key = toks[0].strip()
            val = toks[1].strip()
            if key == "initialdir":
                job_root = val
            elif key == "transfer_output_files":
                output_fname = val
        output_f = join(job_root, output_fname)
        if not isfile(output_f) or getsize(output_f) == 0:
            error_files.add(output_f)
            error_chunks.append(chunk)
            #print "Found an error file!!"
            #print output_f 

    submit_f_str = "\n".join(chunks[0])
    submit_f_str += "\n\n"
    for chunk in error_chunks:
        submit_f_str += "\n".join(chunk)        
        submit_f_str += "\n\n"

    resubmit_f = join(condor_root, "retry_failed_jobs.submit")
    with open(resubmit_f, 'w') as f:
        f.write(submit_f_str)

    print "%d errors" % len(error_files)

    
            
    
    
    
if __name__ == "__main__":
    main()
