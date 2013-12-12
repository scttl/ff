#!/usr/bin/env python
""" @namespace record_value_filter
Perform record filtering of arbitrary data stream based on passed set of 
unacceptable input field values (header determines field name to match
against).
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
    p.add_argument("-v", "--invert", action='store_true',
                   help="invert filter.  Only keep lines matching field values",
                   default=False)
    p.add_argument("-f", "--filterfile", action='append',
                   help="name of a file to use for filtering.  Can specify " +
                   "multiple files (one per -f).  If so we take " +
                   "union.  Each field specifies filterable values for a " +
                   "single field, must match header names")
    p.add_argument("infile", metavar="INFILE", nargs='?', default="-",
                   help="apply filtering to INFILE (instead of stdin)")
    return p


def setup_filters(filter_files, delim='\t'):
    """
    Parses each field of each file in filter_files, constructing a dictionary
    of dictionaries based on the values read.
    @param filter_files list of filenames to read.
    @param delim field separator in each file.  Defaults to tab.
    @return dictionary of dictionaries, the first of which is keyed by the
    field names (assumed to occupy the first row of each file).  Child
    dictionaries then index each value specified for that field.
    """
    d = dict()
    for fname in filter_files:
        f = fileinput.input(fname, openhook=fileinput.hook_compressed)
        for ln in f:
            rec = ln.rstrip('\n').rstrip('\r').split(delim)
            if f.isfirstline():
                hdr = rec[:]
                for h in hdr:
                    if h not in d:
                        d[h] = dict()
            else:
                for i in xrange(len(rec)):
                    d[hdr[i]][rec[i]] = 1
    return d


def file_filter(fname="-", filterfiles=None, invert=False, delim="\t"):
    """
    Processes the named file containing raw merged RealCore data.
    @param fname the name of the file to read from.  Defaults to stdin
    @param filterfiles list of filenames to open and filter by.
    @param invert Invert the filter meaninng.  If TRUE only keep those lines
           that match at least one of the filter values.  Defaults to FALSE
           (only filter out lines matching at least one of the FILTER values).
    @param delim the field separator.  Defaults to tab character
    @return nothing (results are printed to stdout)
    @throws EmptyStdinError if nothing is waiting at stdin and no other files
            are specified.
    @throws NoFiltersError if no filter files have been specified
    """
    f = fileinput.input(fname, openhook=fileinput.hook_compressed)
    filters = dict()
    if fname == "-" and os.isatty(0):
        raise EmptyStdinError("stdin empty")
    if len(filterfiles) == 0:
        raise NoFiltersError("no filter files specified")
    else:
        filters = setup_filters(filterfiles, delim)
    for ln in f:
        keep_line = not invert
        ln = ln.rstrip("\n").rstrip("\r")
        if (f.isfirstline()):
            # determine header fields to match against
            hdrs = ln.split(delim)
            filter_idcs = list()
            for i in xrange(len(hdrs)):
                if hdrs[i] in filters:
                    filter_idcs.append((hdrs[i], i))
            keep_line = True
        else:
            rec = ln.split(delim)
            for h,idx in filter_idcs:
                try:
                    match = rec[idx] in filters[h]
                except IndexError:
                    # can come about if fewer columns in a row
                    continue
                if invert:
                    if match:
                        keep_line = True
                        break
                else:
                    if match:
                        keep_line = False
                        break
        if keep_line:
            print ln


def main():
    """ Point of code entry. """
    parser = prep_arg_parser()
    args = parser.parse_args()
    try:
        file_filter(args.infile, args.filterfile, args.invert, args.delim)
    except EmptyStdinError:
        print("warning: no files specified and nothing waiting at stdin")
        parser.print_help()


if __name__ == '__main__':
    main()
