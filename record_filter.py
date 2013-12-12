#!/usr/bin/env python
""" @namespace record_filter
Perform record filtering of arbitrary data stream based on passed set of 
binary valued input files (should have same length as input being filtered)
"""

## file version
__version__ = "1.0.1"

import sys
import os
import re
import argparse
import fileinput
import signal


def signal_handler(signal, frame):
    """
    Exit cleanly on keyboard interrupt etc.
    """
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGPIPE, signal_handler)


class EmptyStdinError(Exception):
    """
    Raised when user specifies reading from STDIN but no data waiting
    to be read.
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return(repr(self.value))

class NoFiltersError(Exception):
    """
    Raised when user doesn't specify any filter files.
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return(repr(self.value))


def prep_arg_parser():
    """
    Define any command line arguments passed to the script.
    @return argparse.ArgumentParser instance
    """
    p = argparse.ArgumentParser(description=re.sub("@.*\n", "", __doc__))
    p.add_argument("-V", "--version", dest="version",
                   help="display released version number of this script",
                   action="version", version="%(prog)s: " + __version__)
    p.add_argument("-d", "--delimiter", dest="delim",
                   help=r"use DELIM as field separator (instead of \t)",
                   default="\t")
    p.add_argument("-H", "--header", dest="header", action='store_true',
                   help="first line of file is a header to be passed through",
                   default=False)
    p.add_argument("-f", "--filterlist", nargs='+',
                   help="name of a file to use for filtering.  Can specify " +
                   "multiple files (separate by space).  If so we take " +
                   "intersection.  Prepend filename with '_' to invert values")
    p.add_argument("infile", metavar="INFILE", nargs='?', default="-",
                   help="apply filtering to INFILE (instead of stdin)")
    return p


def read_next(filter_files, delim='\t'):
    """
    Reads the next line from each file in the list, and interprets the 
    intersection of their value to determine whether to keep or filter
    the line of input.
    @param filter_files list of tuples contianing open file descriptor, and
           boolean invert meaning pairs.  If the boolean is True we invert the
           0 and 1 value meanings of this file
    @return True if file should be kept, and False otherwise.  If no
    filter_files present, we return True.
    """
    idx = 0
    keep = True
    if filter_files is None or len(filter_files) == 0:
        return True
    while keep and idx < len(filter_files):
        ln = filter_files[idx][0].readline().rstrip('\n').rstrip('\r')
        f_inv = filter_files[idx][1]
        for field in ln.split(delim):
            f_val = field.strip().lower()
            if ((not f_inv and f_val in ["0", "n", "no", "false", ""]) or
               (f_inv and f_val in ["1", "y", "yes", "true"])):
                keep = False
        idx += 1
    while idx < len(filter_files):
        filter_files[idx][0].readline()
        idx += 1
    return keep


def file_filter(fname="-", filterfiles=None, delim="\t", header=False):
    """
    Processes the named file
    @param fname the name of the file to read from.  Defaults to stdin
    @param filterfiles list of filenames to open and filter by.  Each name
           may optionally be prepended with '_' to invert mapping values.
    @param delim the field separator.  Defaults to tab character
    @param header first line of file contains header information to be passed
           through as-is.  Defaults to False
    @return nothing (results are printed to stdout)
    @throws EmptyStdinError if nothing is waiting at stdin and no other files
            are specified.
    @throws NoFiltersError if no filter files have been specified
    """
    f = fileinput.input(fname, openhook=fileinput.hook_compressed)
    if fname == "-" and os.isatty(0):
        raise EmptyStdinError("stdin empty")
    if len(filterfiles) == 0:
        raise NoFiltersError("no filter files specified")
    else:
        ffiles = [(fileinput.input(x.lstrip('_'), 
                                   openhook=fileinput.hook_compressed),
                   True if x.startswith('_') else False) for x in filterfiles]
    for ln in f:
        ln = ln.rstrip("\n").rstrip("\r")
        keep_line = read_next(ffiles)
        if keep_line or (f.isfirstline() and header):
            print ln


def main():
    """ Point of code entry. """
    parser = prep_arg_parser()
    args = parser.parse_args()
    try:
        file_filter(args.infile, args.filterlist, args.delim, args.header)
    except EmptyStdinError:
        print("warning: no files specified and nothing waiting at stdin")
        parser.print_help()


if __name__ == '__main__':
    main()
