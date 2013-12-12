#!/usr/bin/env python
""" @namespace keyed_join
Join together two datasets based on a set of common key values.
Write results to stdout.
"""

## file version
__version__ = "1.2.0"

import sys
import os
import re
import argparse
import fileinput
import locale
import signal
import operator
import string
from collections import defaultdict


def signal_handler(signal, frame):
    """
    Exit cleanly on keyboard interrupt etc.
    """
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGPIPE, signal_handler)

# setup locale to allow comma separated value printing
locale.setlocale(locale.LC_ALL, 'en_US')


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
    p = argparse.ArgumentParser(description=re.sub("@.*\n", "", __doc__),
                                epilog=("By default we perform an " +
                                        "intersection on all common field " +
                                        "names.  We also assume the first " +
                                        "row of each file contains field " +
                                        "names."))
    p.add_argument("-V", "--version", dest="version",
                   help="display released version number of this script",
                   action="version", version="%(prog)s: " + __version__)
    p.add_argument("-d", "--delimiter", dest="delim",
                   help=r"use DELIM as field separator (instead of \t)",
                   default="\t")
    p.add_argument("-i", "--ignore_case", action='store_true',
                   help="ignore case differences in names when matching")
    p.add_argument("-l", "--left_outer", action='store_true',
                   help="Perform left outer join instead of intersection")
    p.add_argument("-m", "--mv", metavar="F1=F2", action='append',
                   help="Rename field F1 to F2 prior to merging.")
    p.add_argument("-r", "--rm", action='append', metavar="F1",
                   help="Remove field named F1 from merged output.")
    p.add_argument("-k", "--key", action='append', metavar="F1",
                   help="add a new join field named F1.")
    p.add_argument("-c", "--closest", action='append', metavar="F1",
                   help="%s %s %s" % (
                   "Constrain multiple key matches by selecting the record",
                   "with the value for field F1 that is closest to the F1",
                   "value being joined"))
    p.add_argument("-s", "--seconstraint", action='append', metavar="F1",
                   help="%s %s %s" % (
                   "Constrain multiple key matches by selecting the record",
                   "with the largest value for field F1 that is <= to the F1",
                   "value being joined"))
    p.add_argument("-S", "--sconstraint", action='append', metavar="F1",
                   help="%s %s %s" % (
                   "Constrain multiple key matches by selecting the record",
                   "with the largest value for field F1 that is < to the F1",
                   "value being joined"))
    p.add_argument("-g", "--geconstraint", action='append', metavar="F1",
                   help="%s %s %s" % (
                   "Constrain multiple key matches by selecting the record",
                   "with the smallest value for field F1 that is >= to the F1",
                   "value being joined"))
    p.add_argument("-G", "--gconstraint", action='append', metavar="F1",
                   help="%s %s %s" % (
                   "Constrain multiple key matches by selecting the record",
                   "with the smallest value for field F1 that is > to the F1",
                   "value being joined"))
    p.add_argument("file", metavar="FILE", nargs='*', default="-",
                   help="read input from FILE.  You can use '-' to " +
                   "specify stdin explicitly but it will also be checked")
    return p


def rename_fields(hdr_lists, rename_strings, ignore_case=False):
    """
    The names within each header in hdr_lists are renamed according to
    the rename_strings.
    @param hdr_lists A list containing lists of strings specifying header names
    @param rename strings A list containing strings of the form FROM=TO where
           FROM and TO are replaced by header name values that we map FROM TO
    @param ignore_case Should we ignore the case when mapping FROM to TO?  By
           default, we assume mappings are case sensitive
    @return Nothing.  hdr_lists is updated in place
    """
    if rename_strings is not None:
        renames = [x.split('=') for x in rename_strings]
        if ignore_case:
            renames = [(x[0].lower(), x[1]) for x in renames]
        d = dict(renames)
        for hdr in hdr_lists:
            for idx in xrange(len(hdr)):
                v = hdr[idx].lower() if ignore_case else hdr[idx]
                hdr[idx] = d.setdefault(v, hdr[idx])


def extract_named_vals(values, names, keep_names=None, rm_names=None):
    """
    Extract a subset of the values based on matching names to be kept or
    dropped.  Generally you would specify one of an explicit list of
    keep_names or rm_names.
    @param values list of values
    @param names list of field names
    @param keep_names list of field names to be explicitly kept.
    @param rm_names list of field names to be explicitly dropped.
    @return tuple of the kept/non-dropped values
    """
    res = []
    for idx in xrange(len(values)):
        if ((keep_names is not None and names[idx] in keep_names) or
           (rm_names is not None and names[idx] not in rm_names)):
            res.append(values[idx])
    return tuple(res)


def create_join_dict(f, headers, keys, rm_vals=None, delim='\t'):
    """
    Construct and return a dictionary from the contents of file f.
    keys should be a tuple based on keys (a subset of headers).  Remaining
    fields are the values unless they are to be removed via rm_vals.
    @param f an open file object ready for reading
    @param headers list of strings containing field header names
    @param keys list of strings containing field header names to use as
           dictionary keys
    @param rm_vals list of strings containing field header names to
           drop from list of fields
    @param delim field delimiter to split f by
    @return new dictionary contructed from the contents of f.
    """
    print_every = 1000
    d = defaultdict(list)
    sys.stderr.write("building lookup dictionary from: %s\n" % f.filename())
    rm = keys + rm_vals if rm_vals is not None else keys
    k_idcs = extract_named_vals(range(len(headers)), headers, keys)
    v_idcs = extract_named_vals(range(len(headers)), headers, None, rm)
    for ln in f:
        if f.filelineno() % print_every == 0:
            sys.stderr.write(locale.format("%d", f.filelineno(), grouping=True)
                             + "\r")
        rec = ln.rstrip('\n').split(delim)
        d[tuple([rec[x] for x in k_idcs])].append(tuple(
                [rec[x] for x in v_idcs]))
    sys.stderr.write("\n")
    return d


def select_first(items, field_idcs=None, cmp_vals=None):
    """
    Given a tuple of tuples in items, selects and returns the first item
    as a tuple, or a tuple containing the empty string if items is empty
    """
    res = True
    try:
        vals = items[0]
    except (TypeError, IndexError):
        vals = '',
        res = False
    return (vals, res)


def select_next_smallest(items, field_idcs, cmp_vals):
    return select_next_op(items, field_idcs, cmp_vals, operator.le,
                          operator.gt)


def select_strictly_next_smallest(items, field_idcs, cmp_vals):
    return select_next_op(items, field_idcs, cmp_vals, operator.lt,
                          operator.gt)


def select_next_largest(items, field_idcs, cmp_vals):
    return select_next_op(items, field_idcs, cmp_vals, operator.ge,
                          operator.lt)


def select_strictly_next_largest(items, field_idcs, cmp_vals):
    return select_next_op(items, field_idcs, cmp_vals, operator.gt,
                          operator.lt)


def select_next_op(items, field_idcs, cmp_vals, this_cmp_op, replace_op):
    """
    Uses this_cmp_op to compare the field_idcs values of items with cmp_vals
    for validity.  For each valid comparison we keep the single item that
    last matches the replace_op comparison.  We return this matching item
    if a valid match is found, otherwise we return a tuple of empty string
    the length of the first item.
    Typically this_cmp_op and replace_op are given operator.le, lt, ge, or gt
    """
    res = None
    res_vals = None
    valid = False
    for item in items:
        this_vals = [item[x] for x in field_idcs]
        if this_cmp_op(this_vals, cmp_vals):
            # this item is a valid selection
            if res_vals is None or replace_op(this_vals, res_vals):
                valid = True
                res = item
                res_vals = this_vals
    if res is None:
        if items is not None and len(items) > 0:
            res = tuple([''] * len(items[0]))
        else:
            res = '',
    return (res, valid)


def select_closest(items, field_idcs, cmp_vals):
    """
    Attempt to convert the values to numerics and find the single closest match
    """
    res = None
    res_diffs = None
    valid = False
    ncmp_vals = numericize(cmp_vals)
    for item in items:
        this_vals = numericize([item[x] for x in field_idcs])
        diffs = [abs(x[0] - x[1]) for x in zip(ncmp_vals, this_vals)]
        if res_diffs is None or diffs < res_diffs:
            valid = True
            res = item
            res_diffs = diffs
    if res is None:
        if items is not None and len(items) > 0:
            res = tuple([''] * len(items[0]))
        else:
            res = '',
    return (res, valid)


def numericize(x, defval=float("nan")):
    """
    Return a copy of x with its component values converted to a numeric
    representation.  We try hard to strip characters and punctuation before
    converting but if no numeric content remains, it gets assigned the default
    value
    """
    res = defval
    rmchars = (string.letters + string.whitespace + 
               '!"#$%&\'()*+,/:;<=>?@[\\]^_`{|}~')
    try:
        res = float(x)
    except ValueError:
        # likely a string
        try:
            test_str = x
            if test_str.count('.') >= 1:
                first_dot = test_str.index('.') + 1
                test_str = (test_str[:first_dot] + 
                            test_str[first_dot:].replace('.',''))
            if len(test_str) > 0 and test_str[0] == '-':
                test_str = '-' + test_str.replace('-', '')
            else:
                test_str = test_str.replace('-', '')
            res = float(test_str.translate(None, rmchars))
        except ValueError:
            res = defval
    except TypeError:
        # likely some type of sequence
        if hasattr(x, '__iter__'):
            res = [numericize(i, defval) for i in x]
    return res


def join_files(files, keys=None, mv=None, rm=None, delim="\t",
               ignore_case=False, outer_join=False,
               multi_match_fn=None, multi_match_fields=[]):
    """
    Merge the named files.
    @param files List of names of the file to read from.  We will also
           check stdin for data.
    @param keys List of field names to use as keys.  If not specified we use
           all common keys for matching.
    @param mv List of f1=f2 value strings specifying the names of fields to
           rename to new values.  This happens prior to merging.
    @param rm List of field names to remove from the merged output.
    @param delim the field separator.  Defaults to tab character
    @param ignore_case should we ignore case difference when comparing field
           names for merging?  Defaults to False i.e. treat Field1 and field1
           as different fields.
    @param outer_join Should we perform a left outer join on this data (instead
           of default inner join i.e. intersection)?  If we do, fields of
           subsequent files where no match exists are written as empty
           strings.  Defaults to False.
    @param multi_match_fn Determines how to select a single item in cases where
           multiple records exist for the same key.  If None, will take
           first item found.
    @param multi_match_fields tuple listing the fields to sort the multi match
           tuples by
    @return nothing (results are printed to stdout)
    @throws EmptyStdinError if nothing is waiting at stdin and no other files
            are specified.
    """
    print_every = 1000
    # open each file, read first row to get headers
    if (files is None or len(files) == 0 or files[0] == "-") and os.isatty(0):
        raise EmptyStdinError("stdin empty")
    if "-" not in files:
        files.insert(0, "-")
    fs = [fileinput.input(x, openhook=fileinput.hook_compressed)
          for x in files]
    hdrs = [x.readline().rstrip("\n").split(delim) for x in fs]
    # rename fields
    rename_fields(hdrs, mv, ignore_case)
    rename_fields([multi_match_fields], mv, ignore_case)
    if keys is not None:
        rename_fields([keys], mv, ignore_case)
    # get list of merge key fields
    hmerge = hdrs[:]
    if ignore_case:
        for idx in xrange(len(hmerge)):
            hmerge[idx] = [x.lower() for x in hmerge[idx]]
    set_hdrs = [set(x) for x in hmerge]
    if keys is None:
        merge_key_set = set.intersection(*set_hdrs)
        merge_keys = [x for x in hmerge[0] if x in merge_key_set]
    else:
        merge_keys = keys
    sys.stderr.write('join key field(s): %s\n' % merge_keys)
    if multi_match_fn is None:
        sys.stderr.write('multi-match function: take first\n')
    else:
        sys.stderr.write('multi-match function: %s, tiebreaker fields: %s\n' %
                         (multi_match_fn.__name__, str(multi_match_fields)))
    rm_keys = [x.lower() for x in rm] if ignore_case and rm is not None else rm
    if rm_keys is None:
        rm_keys = []
    # remove non-keep columns, read in all files but first, creating dicts
    fh_tuples = zip(fs, hmerge)
    lookup_dicts = [create_join_dict(f, h, merge_keys, rm_keys, delim)
                    for (f, h) in fh_tuples[1:]]
    # prepare header
    hdr = []
    for idx in xrange(len(hdrs)):
        for h_idx in xrange(len(hdrs[idx])):
            if (hmerge[idx][h_idx] not in rm_keys and (idx == 0 or
               hmerge[idx][h_idx] not in merge_keys)):
                val = hdrs[idx][h_idx]
                if idx == 0 and val in multi_match_fields:
                    val = "LEFT_MM_" + val
                hdr.append(val)
    print delim.join(hdr)
    # get multi-match field indices
    mm_idcs = [None] * len(hmerge)
    for idx in xrange(len(mm_idcs)):
        mm_idcs[idx] = tuple([hmerge[idx].index(x) for x in multi_match_fields
                              if x in hmerge[idx]])
    if outer_join:
        # prep outer-join empty tuples for appends
        oj_empties = []
        for idx in range(len(lookup_dicts)):
            num = len([x for x in hmerge[idx+1]
                       if x not in rm_keys + merge_keys])
            oj_empties.append(tuple([''] * num))
        sys.stderr.write("oj_empties: %s\n" % str(oj_empties))
    # prep various indices for faster lookup/iteration
    first_fields = extract_named_vals(hmerge[0], hmerge[0], None,
                                      merge_keys + rm_keys)
    key_val_idcs = extract_named_vals(range(len(hmerge[0])), hmerge[0],
                                      merge_keys)
    val_idcs = extract_named_vals(range(len(hmerge[0])), hmerge[0],
                                  first_fields)
    # iterate through lines of first file, printing details as required
    sys.stderr.write("merging against lines of %s\n" % fs[0].filename())
    merge_count = 1  # +1 for the header
    for ln in fs[0]:
        if fs[0].filelineno() % print_every == 0:
            sys.stderr.write(locale.format("%d", fs[0].filelineno(),
                             grouping=True) + "\r")
        rec = ln.rstrip('\n').split(delim)
        key_vals = tuple([rec[x] for x in key_val_idcs])
        vals = tuple([rec[x] for x in val_idcs])
        valid_merge = True
        for idx in xrange(len(lookup_dicts)):
            if key_vals in lookup_dicts[idx]:
                if multi_match_fn is None:
                    (newvals, res) = (lookup_dicts[idx][key_vals][0], True)
                else:
                    (newvals, res) = multi_match_fn(lookup_dicts[idx][
                                                    key_vals],
                                                    mm_idcs[idx],
                                                    [rec[x] for x in
                                                     mm_idcs[0]])
                vals += newvals
                if not res:
                    merge_count -= 1
            elif outer_join:
                merge_count -= 1
                vals += oj_empties[idx]
            else:
                valid_merge = False
                break
        if valid_merge:
            merge_count += 1
            val_idx = 0
            ord_vals = []
            first_h_list = True
            for h_list in hmerge:
                for h in h_list:
                    if first_h_list:
                        try:
                            ord_vals.append(key_vals[merge_keys.index(h)])
                        except ValueError:
                            ord_vals.append(vals[val_idx])
                            val_idx += 1
                    else:
                        if h not in merge_keys:
                            ord_vals.append(vals[val_idx])
                            val_idx += 1
                first_h_list = False
            print delim.join(ord_vals)
    sys.stderr.write('\n')
    sys.stderr.write("final number of merged records: " +
                     locale.format("%d", merge_count, grouping=True) + "\n")


def assign_multi_match_handler(slist, selist, glist, gelist, cllist):
    fn = None
    sort_fields = ()
    if slist is not None and len(slist) > 0:
        fn = select_strictly_next_smallest
        sort_fields = slist
    elif selist is not None and len(selist) > 0:
        fn = select_next_smallest
        sort_fields = selist
    elif glist is not None and len(glist) > 0:
        fn = select_strictly_next_largest
        sort_fields = glist
    elif gelist is not None and len(gelist) > 0:
        fn = select_next_largest
        sort_fields = gelist
    elif cllist is not None and len(cllist) > 0:
        fn = select_closest
        sort_fields = cllist
    return (fn, sort_fields)


def main():
    """ Point of code entry. """
    parser = prep_arg_parser()
    args = parser.parse_args()
    try:
        (multi_fn, multi_flds) = assign_multi_match_handler(args.sconstraint,
                                                            args.seconstraint,
                                                            args.gconstraint,
                                                            args.geconstraint,
                                                            args.closest)
        join_files(args.file, args.key, args.mv, args.rm, args.delim,
                   args.ignore_case, args.left_outer, multi_fn, multi_flds)
    except EmptyStdinError:
        print("warning: no files specified and nothing waiting at stdin")
        parser.print_help()


if __name__ == '__main__':
    main()
