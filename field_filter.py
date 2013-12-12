#!/usr/bin/env python
""" @namespace field_filter
Perform named field filtering of arbitrary data stream based on passed set of
field names to keep.  We assume the first record of the stream contains header
names.
"""

## file version
__version__ = "1.0.2"

import sys
import os
import re
import argparse
import fileinput
from operator import itemgetter
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


class InvalidFieldIndexError(Exception):
    """
    Raised when trying to extract fields that don't exist in a given record.
    Often because that record has a different number of fields than the header
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
    p.add_argument("-k", "--keeplist", nargs='+',
                   help="names of fields to keep (separate by space).")
    p.add_argument("-f", "--filterlist", nargs='+',
                   help="names of fields to remove (separate by space).")
    p.add_argument("infile", metavar="INFILE", nargs='?', default="-",
                   help="apply filtering to INFILE (instead of stdin)")
    return p


def field_filter(fname="-", delim="\t", keeplist=None, filterlist=None):
    """
    Processes the named file, keeping or removing fields based on list of field
    names.  We assume the first row is a header
    @param fname the name of the file to read from.  Defaults to stdin
    @param delim the field separator.  Defaults to tab character
    @param keeplist explicit list of field names to keep.  Has higher priority
           than filterlist
    @param filterlist explicit list of field names to remove.
    @return nothing (results are printed to stdout)
    @throws EmptyStdinError if nothing is waiting at stdin and no other files
            are specified.
    @throws InvalidFieldIndexError after processing indicating how many records
            had an invalid number of fields which prohibited parsing them.
    """
    f = fileinput.input(fname, openhook=fileinput.hook_compressed)
    invalid_count = 0
    if fname == "-" and os.isatty(0):
        raise EmptyStdinError("stdin empty")
    for ln in f:
        ln = ln.rstrip("\n")
        if f.isfirstline():
            fld_names = [x.strip().lower() for x in ln.split(delim)]
            keep_idcs = xrange(len(fld_names))
            flds = dict(zip(fld_names, keep_idcs))
            if filterlist is not None:
                filterlist = [x.lower() for x in filterlist]
                filter_idcs = [flds[x] for x in filterlist if x in flds]
                keep_idcs = [x for x in keep_idcs if x not in filter_idcs]
            if keeplist is not None:
                keeplist = [x.lower() for x in keeplist]
                keep_idcs = [flds[x] for x in keeplist if x in flds]
            field_getter = (itemgetter(*keep_idcs) if len(keep_idcs) > 1
                            else lambda ar: [ar[keep_idcs[0]]])
        try:
            print delim.join(field_getter(ln.split(delim)))
        except IndexError:
            sys.stderr.write("failed to get %d fields from %d length line: " 
                             % (len(keep_idcs), len(ln.split(delim))))
            sys.stderr.write("'%s', indexes: '%s'\n" % (ln, str(keep_idcs)))
            invalid_count += 1
    if invalid_count > 0:
        raise InvalidFieldIndexError("unable to process %d invalid records" %
                                     invalid_count)


def main():
    """ Point of code entry. """
    parser = prep_arg_parser()
    args = parser.parse_args()
    try:
        field_filter(args.infile, args.delim, args.keeplist, args.filterlist)
    except EmptyStdinError:
        print("warning: no files specified and nothing waiting at stdin")
        parser.print_help()
    except InvalidFieldIndexError as e:
        sys.stderr.write(str(e))


if __name__ == '__main__':
    main()
