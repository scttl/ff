#!/usr/bin/env python
""" 
Perform reservoir sampling on an input stream of unbounded length
"""

## file version
__version__ = "0.0.1"

import sys
import os
import random
import fileinput
import argparse


class Usage(Exception):
    """
    Program usage exception.
    """
    def __init__(self, msg):
        """
        Create a new Usage instance.
        @param self object
        @param msg String giving additional message to display prior to usage.
        @return created instance object
        """
        ## default message
        self.msg = str(msg) + "\n\n" + __doc__[__doc__.find(".")+2:]

def prep_arg_parser():
    """
    Sets up and parses the passed command line arguments using argparse.
    @return argparse.ArgumentParser instance
    """
    p = argparse.ArgumentParser(
            description="Randomly sample a subset of records in a single pass.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("-V", "--version", dest="version",
                   help="display released version number of this tool.",
                   action="version", version="%(prog)s: " + __version__)
    p.add_argument("-H", "--header", action="store_true", default=False,
                   help="the first line of the file contains header information (and should always be included).")
    p.add_argument("-n", "--num", type=int, default=1,
                   help="number of samples to collect")
    p.add_argument("-s", "--seed", type=int, default=None,
                   help="seed value for random number generator.")
    p.add_argument("files", metavar="FILE", nargs="*", default=["-"],
                   help="read input from FILE")
    return p

class StreamSampler:
    """
    Samples files/STDIN streams uniformly.  Uses the algorithm proposed in
    McLeod & Bellhouse (1983) that allows doing so in a single-pass of an
    unknown number of records.
    """
    def __init__(self, num_samples=1, seed=None, header=False):
        """
        Create and return a new StreamSampler object.
        @param self the object
        @param num_samples integer specifying the number of rows to sample.
               Defaults to 1.
        @param seed integer giving the seed value to the random numer
               generator.  If not specified, defaults to None which will
               randomly assign a value.
        @param header Boolean indicating whether the first row should be
               treated as a header record (and always included in the sample).
               Defaults to False.
        @return created instance object
        """
        ## boolean indicating presence/absence of header in first row
        self.header = header
        ## random number generator seed value.
        self.seed = seed
        try:
            ## the number of samples to take
            self.num_samples = int(num_samples)
            if self.num_samples <= 0:
                raise VAlueError
        except (TypeError, ValueError):
            raise Usage("non-integral or non-positive num. of samples given: %s"
                        % str(num_samples))
        random.seed(self.seed)
        ## header content (if header row present)
        self.head_rec = ""
        ## the actual sampled values
        self.samples = []

    def sample(self, files, print_every=1000):
        """
        Determines the set of sample records from the file names given.
        @param self the object
        @param files list of filenames.  Reads from STDIN if "-" is specified.
        @param print_every Write to STDERR the record number every print_every
               lines.  Defaults to 1000.  Set to 0 to disable printing
               altogether.
        """
        recnum = 0
        try:
            for ln in fileinput.input(files):
                if self.header and fileinput.filelineno() == 1:
                    self.head_rec = ln
                else:
                    recnum += 1
                    if print_every > 0 and recnum % print_every == 0:
                        sys.stderr.write("%d\r" % recnum)
                    if recnum <= self.num_samples:
                        self.samples.append(ln)
                    else:
                        idx = int(random.random() * recnum)
                        if idx < self.num_samples:
                            self.samples[idx] = ln
        except IOError, msg:
            raise Usage("Problem reading from file '%s':\n%s" % 
                        (fileinput.filename(), msg))

    def print_samples(self):
        """
        Writes sampled records to STDOUT.
        @param self the object
        """
        if self.header:
            print self.head_rec,
        for ln in self.samples:
            print ln,


def main(args):
    """
    Randomly sample an input stream of records uniformly without replacement.
    @param args set of command-line arguments (as produced by sys.argv)
    @return 0 on success or positive integer indicating error.
    """
    try:
        #grab arguments
        #(files, num_samples, seed, header) = _handle_args(args[1:])
        parser = prep_arg_parser()
        args = parser.parse_args()
        #sample each file
        s = StreamSampler(args.num, args.seed, args.header)
        s.sample(args.files)
        #display the results
        s.print_samples()
    except Usage, err:
        print >>sys.stderr, err.msg
        return 2
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
