#!/usr/bin/env python
""" @namespace zpaste.py
Emulate the standard linux paste command (allowing one to merge files
horizontally or vertically) but add transparent support for gzipped input
files.
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


def prep_arg_parser():
    """
    Define any command line arguments passed to the script.
    @return argparse.ArgumentParser instance
    """
    p = argparse.ArgumentParser(description=re.sub("@.*\n", "", __doc__))
    p.add_argument("-V", "--version", dest="version",
                   help="display released version number of this script",
                   action="version", version="%(prog)s: " + __version__)
    p.add_argument("-d", "--delimiters", dest="delims",
                   help="%s %s" % ("reuse characters from this as field",
                                   r"separators (instead of \t)"),
                   default="\t")
    p.add_argument("-s", "--serial", action='store_true',
                   help="paste one file at a time instead of in parallel")
    p.add_argument("files", metavar="FILE", nargs='*', default=["-"],
                   help="apply filtering to each FILE (instead of stdin)")
    return p


def zpaste(files=["-"], delims="\t", serial=False):
    """
    Merges the lines of the named file.
    @param files list of (possibly compressed) files to read from.  Defaults to
           stdin
    @param delims a string of field separators.  Each character is recycled as
           required.  Defaults to tab character
    @param serial Perform the merge one file at time (i.e. horizontal merge).
            Defaults to False
    @return nothing (results are printed to stdout)
    @throws EmptyStdinError if nothing is waiting at stdin and no other files
            are specified.
    """
    if "-" in files and os.isatty(0):
        raise EmptyStdinError("stdin empty")
    fs = [fileinput.input(x, openhook=fileinput.hook_compressed) for x in
          files]
    ds = list(delims)
    didx = 0
    fidx = 0
    if serial:
        while fidx < len(fs):
            for ln in fs[fidx]:
                ln = ln.rstrip()
                if not fs[fidx].isfirstline():
                    sys.stdout.write(ds[didx])
                    didx = (didx + 1) % len(ds)
                sys.stdout.write(ln)
            sys.stdout.write('\n')
            didx = 0
            fidx += 1
    else:
        openfs = True
        while openfs:
            openfs = False
            outline = ''
            for fidx in xrange(len(fs)):
                if fidx != 0:
                    outline += ds[didx]
                    didx = (didx + 1) % len(ds)
                ln = fs[fidx].readline()
                if ln != '':
                    openfs = True
                outline += ln.rstrip('\n')
            didx = 0
            if openfs:
                sys.stdout.write(outline + '\n')


def main():
    """ Point of code entry. """
    parser = prep_arg_parser()
    args = parser.parse_args()
    try:
        zpaste(args.files, args.delims, args.serial)
    except EmptyStdinError:
        print("warning: no files specified and nothing waiting at stdin")
        parser.print_help()


if __name__ == '__main__':
    main()
