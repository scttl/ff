#!/usr/bin/env python
""" @namespace keyed_stats
Construct indexed tables of statistics according to various types of keys
"""

## file version
__version__ = "1.0.3"

import sys
import os
import re
import argparse
import fileinput
import datetime
import string
import signal
import locale
from collections import defaultdict
from dateutil.relativedelta import relativedelta
from itertools import product
from multiprocessing import Pool


def signal_handler(signal, frame):
    """Exit cleanly on keyboard interrupt etc."""
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


class FieldNotFoundError(Exception):
    """
    Raised when user specifies a field name or offset that doesn't exist
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return(repr(self.value))


class StatList:
    """
    A collection of KeyedRecords upon which statistics can be computed.
    """
    def __init__(self):
        self.krs = list()
        self.is_sorted = True

    def add(self, keyed_rec):
        """
        Add a new KeyedRecord to this stats list.
        @param keyed_rec a KeyedRecord instance.
        """
        self.krs.append(keyed_rec)
        self.is_sorted = False

    def get_count(self):
        """
        Return the number of items currently in this list.
        @return int specifying the item count
        """
        return len(self.krs)

    def get_min(self):
        """
        Return the minimum value of the items currently in this list.
        @return float specifying the min value.
        """
        res = float('inf')
        for kr in self.krs:
            if isinstance(kr.val, (int, float)):
                res = min(res, kr.val)
        if res == float('inf'):
            res = float('nan')
        return res

    def get_max(self):
        """
        Return the maximum value of the items currently in this list.
        @return float specifying the min value.
        """
        res = float('-inf')
        for kr in self.krs:
            if isinstance(kr.val, (int, float)):
                res = max(res, kr.val)
        if res == float('-inf'):
            res = float('nan')
        return res

    def get_mean(self):
        """
        Return the average of the numeric values in this list.
        @return float specifying average value.
        """
        tot = 0.0
        count = 0
        res = float('nan')
        for kr in self.krs:
            if isinstance(kr.val, (int, float)):
                tot += kr.val
                count += 1
        if count > 0:
            res = tot / count
        return res

    def get_mode(self):
        """
        Determine the most frequently occuring value in this list.
        @return most frequent value and count as a tuple.  Note that ties
                will be broken by returning the smallest such most frequent
                item.
        """
        if not self.is_sorted:
            self.krs.sort(key=lambda kr: kr.val)
            self.is_sorted = True
        val = None
        count = 0
        max_count = -1
        max_val = None
        for kr in self.krs:
            if kr.val != val:
                if count > max_count:
                    max_count = count
                    max_val = val
                count = 0
                val = kr.val
            count += 1
        # ensure we compare the very last item
        if count > max_count:
            max_count = count
            max_val = val
        return (max_val, max_count)

    def get_percentiles(self, percentiles=[50], only_numeric=False):
        """
        Determine and return the specified percentiles.  Note that missing and
        invalid values are included in the ordering (by default).
        @param percentiles list of numeric values [1-99] specifying
               percentile(s) to compute.  Defaults to median i.e. [50]
        @param only_numeric if True only numeric values in the list are
               examined
        @return list of values corresponding to the percentiles.
        """
        if not self.is_sorted:
            self.krs.sort(key=lambda kr: kr.val)
            self.is_sorted = True
        num_recs = self.get_count()
        if only_numeric:
            idx = 0
            while idx < num_recs and self.krs[idx].val in [None, '']:
                idx += 1
            if idx == num_recs:
                # all indices are invalid/missing
                idx = 0
            percentile_idcs = [idx + int(float(x) / 100 * (num_recs - idx)) for
                               x in percentiles]
        else:
            percentile_idcs = [int(float(x) / 100 * num_recs) for x in
                               percentiles]
        return [self.krs[x].val for x in percentile_idcs]


class KeyedRecord:
    """
    A single line of a KeyedFile containing indexing field and value values.
    """
    def __init__(self, keys, value, dates):
        """
        Create a new record.
        @param keys tuple of key values
        @param value the value upon which stats are calculated
        @param dates tuple of date values
        """
        self.keys = keys
        self.val = value
        self.dates = dates

    def __lt__(self, other):
        return self.val < other.val

    def __repr__(self):
        return "KeyedRecord(keys=%r, value=%r, dates=%r)" % (self.keys,
                                                             self.val,
                                                             self.dates)


class KeyedFile:
    """
    Input file containing key indexing fields and a value field to compute
    statistics over.
    """
    def __init__(self, fname, stat_field, keys, backoffs, delim="\t",
                 header=False, dates=None, month_lag=0, month_prior_group=0,
                 alpha_order=False):
        """
        Create a new instance.
        @param fname name of the file containing fields to process.  Use '-'
               for stdin.  May be gzip compressed.
        @param stat_field 1-based field offset or field name (if header
               present), to identify which field to calculate stats over.
        @param keys list of 1-based field offsets or field names (if header
               present), to identify how to group records together for stat
               computation.  Each unique set of key values will form a row in
               the output table.
        @param backoffs list of strings the same length as keys containing
               filenames of backoff value lookup files, used to expand counts
               for a given value.  If no backoff file exists, pass in None
               for the corresponding key.
        @param delim field separator.  Defaults to tab.
        @param header Does the first line contain field names?  Defaults to
               False.
        @param dates, list of 1-based field offsets or field names (if header
               present), to identify which key fields are dates.  The values
               are assumed to be in YYYYMMDD or YYYYMM format (after stripping
               any punctuation and space characters).
        @param month_lag if any date fields are specified, set this to a
               positive integer to group entries from month_lag months
               earlier.  So if month_lag is 2, the entry for 201306 will be
               constructed from data ending 201304.  Defaults to 0 for no lag
        @param month_prior_group if any date fields are specifed, set this to a
               positive integer to group entries from multiple months together
               when calculating statistics.  If month_prior_group was 6, the
               value for 201309 will include all record values from 201303 -
               201309 inclusive.  Defaults to 0 for no prior months included.
        @param alpha_order boolean indicating whether stat_field values should
               be ordered numerically (False), or alphabetically (True).
               Defaults to False.
        """
        self.f = fileinput.FileInput(fname,
                                     openhook=fileinput.hook_compressed)
        self.delim = delim
        if fname == "-" and os.isatty(0):
            raise EmptyStdinError("stdin empty")
        if header:
            names = self.f.readline().rstrip('\n').rstrip('\r').split(delim)
        else:
            names = []
        self.field = self.names_to_cols(names, stat_field)[0]
        self.keys = self.names_to_cols(names, *keys)
        self.backoffs = self.read_backoffs(*backoffs)
        self.dates = self.names_to_cols(names, *dates)
        self.month_lag = month_lag
        self.month_group = month_prior_group
        self.alpha_order = alpha_order
        if len(self.keys) == 0 and len(self.dates) == 0:
            raise FieldNotFoundError("0 keys specified")

    def read_backoffs(self, *backoffs):
        """
        Read each file specified to construct a lookup hashtable from its
        contents.
        @param backoffs the filenames to read from
        @return tuple of hash tables which will be empty for any
                unspecified/invalid backoff filenames
        """
        res = list()
        for bf in backoffs:
            h = dict()
            if bf is not None:
                try:
                    for ln in fileinput.FileInput(bf,
                            openhook=fileinput.hook_compressed):
                        key, backvals = ln.rstrip('\n').rstrip('\r').split('\t')
                        if len(backvals) > 0:
                            h[key] = backvals.split(' ')
                except TypeError, IOError:
                    pass
            res.append(h)
        return tuple(res)

    def names_to_cols(self, names, *fields):
        """
        Convert the passed list of field names/column offsets according to the
        list of names passed.
        @param names list of fieldnames as strings to match against
        @param fields the values to convert.
        @return 1-based indices of each match in fields (with value None for
                those that aren't found)
        @throws FieldNotFoundError if any field values are invalid
        """
        res = list()
        for f in fields:
            try:
                # see if we can find a match in our list of names
                idx = names.index(f) + 1
            except ValueError:
                try:
                    # assume a column offset was supplied
                    idx = int(f)
                except ValueError:
                    idx = None
            if idx is None or idx > len(names):
                raise FieldNotFoundError(str(f))
            res.append(idx)
        return tuple(res)

    def read_recs(self):
        """
        Extract the relevant key and value fields from each record in this
        file (yielded one at a time)
        @return generator of KeyedRecord objects
        """
        for ln in self.f:
            ln = ln.rstrip('\n').rstrip('\r').split(self.delim)
            val = ln[self.field - 1]
            try:
                val = float(val)
            except ValueError:
                pass
            yield KeyedRecord(tuple(ln[x - 1] for x in self.keys), val,
                              tuple(datetime.datetime.strptime(ln[x - 1
                                    ].replace('-', '').replace(' ', ''),
                                    "%Y%m") for x in self.dates))

    def valid_months(self, dt):
        """
        Compute and return all valid months for a given datetime, based on
        number of prior months to compute and any lag.
        @param dt datetime object specifying year and month
        @return list of datetime objects
        """
        # lag date needs to be added, so an original date of 2013-08 with a 2
        # month lag, would be calculated towards the count for 2013-10
        lagdt = dt + relativedelta(months=self.month_lag)
        return [''] + [(dt + relativedelta(months=x)).strftime('%Y%m') for
                       x in xrange(0, -self.month_group - 1, -1)]

    def all_keysets(self, kr):
        """
        Compute all valid permutations of the keys of the KeyedRecord passed,
        including expanding for blank/missing values and taking into account
        date lag and prior month grouping.
        @param kr the KeyedRecord upon which to calculate the list of key
               permutations
        @return list of key value tuples
        """
        keylists = []
        for idx in xrange(len(kr.keys)):
            k = kr.keys[idx]
            val = [k]
            if k not in ('', None):
                val.append('')
            if k in self.backoffs[idx]:
                val.extend(self.backoffs[idx][k])
            keylists.append(val)
        datelists = [self.valid_months(x) for x in kr.dates]
        keylists.extend(datelists)
        return tuple(product(*keylists))


def calc_stats(data_args):
    """
    Calculate specified statistics given a record, args tuple
    @param data_args, 2-tuple containing the record set and args
    @return string containing computed stats results
    """
    s = data_args[0]
    k = data_args[1]
    args = data_args[2]
    out = args.delim.join([x.strftime('%Y%m') if isinstance(x,
                           datetime.datetime) else str(x) for x in k])
    if args.count:
        out += args.delim + str(s.get_count())
    if args.min:
        out += args.delim + str(s.get_min())
    if args.max:
        out += args.delim + str(s.get_max())
    if args.mean:
        out += args.delim + str(s.get_mean())
    if args.mode:
        out += args.delim + str(s.get_mode()[0])
    if len(args.percentile) > 0:
        out += args.delim + args.delim.join([str(x) for x in
                                             s.get_percentiles(args.percentile,
                                             args.numpercentile)])
    return out


def prep_arg_parser():
    """
    Define any command line arguments passed to the script.
    @return argparse.ArgumentParser instance
    """
    p = argparse.ArgumentParser(description=re.sub("@.*\n", "", __doc__))
    p.add_argument("-V", "--version",
                   help="display released version number of this script",
                   action="version", version="%(prog)s: " + __version__)
    p.add_argument("-d", "--delimiter", dest="delim",
                   help=r"use DELIM as field separator (instead of \t)",
                   default="\t")
    p.add_argument("-H", "--header", action='store_true', default=False,
                   help="first line of file is a header to be passed through")
    p.add_argument("-k", "--key", action='append',
                   help="%s%s%s" % ("add a new field name or offset to index ",
                                    "by, and optionally a backoff file ",
                                    "(separate by comma)"))
    p.add_argument("-f", "--field",
                   help="calculate stats on the field name/offset given")
    p.add_argument("-a", "--alpha", default=False, action='store_true',
                   help="order field values alphabetically instead of numeric")
    p.add_argument("-p", "--percentile", action="append",
                   default=["1", "5", "25", "50", "75", "95", "99"],
                   help="add a new percentile value to field stats")
    p.add_argument("-n", "--numpercentile", action='store_true', default=False,
                   help="strip missing/non-numeric values before calc perc.")
    p.add_argument("-m", "--mean", action='store_true', default=False,
                   help="add mean value to numeric field stats")
    p.add_argument("-M", "--mode", action='store_true', default=False,
                   help="add most common value to field stats")
    p.add_argument("-c", "--count", action='store_true', default=False,
                   help="add record count to field stats")
    p.add_argument("-x", "--min", action='store_true', default=False,
                   help="add min value to field stats")
    p.add_argument("-X", "--max", action='store_true', default=False,
                   help="add max value to field stats")
    p.add_argument("-D", "--date", action='append', default=list(),
                   help="the field name/offset is a YYYYMM[DD] date")
    p.add_argument("-l", "--lag", type=int, default=0,
                   help="compute date indices with a lag of this many months")
    p.add_argument("-w", "--width", type=int, default=0,
                   help="include date values from this many prior months")
    p.add_argument("-P", "--printevery", type=int, default=0,
                   help="dump stats every printeveryth line. disable via 0")
    p.add_argument("-C", "--cores", type=int, default=1,
                   help="calculate stats in parallel using many CPU cores")
    p.add_argument("-R", "--merge", action='append', default=list(),
                   help="list of tables to merge instead of create")
    p.add_argument("infile", metavar="INFILE", nargs='?', default="-",
                   help="construct tables from INFILE (instead of stdin)")
    return p


def merge_tables(fnames, delim="\t", printevery=0):
    """
    Merges the list of already built tables and dumps the results to standard
    output.
    @param fnames a list of filenames containing already created tables of the
           same type
    @param delim field sepearator in the tables
    @param printevery if set to a positive integer the current record count is
           written to std.err
    """
    d = defaultdict(list)
    hdr = None
    key_idcs = list()
    val_idcs = list()
    count_idx = None
    min_idx = None
    max_idx = None
    mean_idx = None
    mode_idx = None
    percentile_idcs = list()
    f = fileinput.FileInput(fnames, openhook=fileinput.hook_compressed)
    for ln in f:
        if printevery > 0 and f.lineno() % printevery == 0:
            sys.stderr.write("file: %s\trec: %s\r" % (
                             os.path.basename(f.filename()),
                             locale.format("%d", f.filelineno(),
                                           grouping=True)))
        rec = ln.rstrip('\n').rstrip('\r').split(delim)
        if f.isfirstline():
            # header line, process if not done so already
            if hdr is None:
                hdr = ln.rstrip('\n').rstrip('\r')
                for idx, val in enumerate(rec):
                    if val.endswith("_count"):
                        count_idx = len(val_idcs)
                        val_idcs.append(idx)
                    elif val.endswith("_min"):
                        min_idx = len(val_idcs)
                        val_idcs.append(idx)
                    elif val.endswith("_max"):
                        max_idx = len(val_idcs)
                        val_idcs.append(idx)
                    elif val.endswith("_mean"):
                        mean_idx = len(val_idcs)
                        val_idcs.append(idx)
                    elif val.endswith("_mode"):
                        mode_idx = len(val_idcs)
                        val_idcs.append(idx)
                    elif val.find("_percentile_") >= 0:
                        percentile_idcs.append(len(val_idcs))
                        val_idcs.append(idx)
                    else:
                        key_idcs.append(idx)
            else:
                if printevery > 0:
                    sys.stderr.write('\n')
        else:
            # append to current dictionary
            d[tuple(rec[x] for x in key_idcs)].append(tuple(rec[x] for x in
                                                            val_idcs))
    if printevery > 0:
        sys.stderr.write('\n')
    # now write out the in-memory structure, merging results
    print(hdr)
    for key in sorted(d.keys()):
        rec = delim.join(key)
        if len(d[key]) == 1:
            rec = delim.join((rec, delim.join(d[key][0])))
        else:
            vals = [""] * len(val_idcs)
            items = zip(*d[key])
            if count_idx is not None:
                vals[count_idx] = sum([int(x) for x in items[count_idx]])
            if min_idx is not None:
                try:
                    vals[min_idx] = min([float(x) for x in items[min_idx]
                                         if x not in ("", "nan")])
                except ValueError:
                    vals[min_idx] = ""
            if max_idx is not None:
                try:
                    vals[max_idx] = max([float(x) for x in items[max_idx]
                                         if x not in ("", "nan")])
                except ValueError:
                    vals[max_idx] = ""
            if mean_idx is not None:
                if count_idx is not None:
                    weights = (float(x) / vals[count_idx] for x in
                               items[count_idx])
                else:
                    weights = ([1. / len(items[mean_idx])] *
                               len(items[mean_idx]))
                try:
                    vals[mean_idx] = sum(x[0] * float(x[1]) for x in
                                         zip(weights, items[mean_idx])
                                         if x[1] not in ("", "nan"))
                    if vals[mean_idx] == 0:
                        vals[mean_idx] = ""
                except ValueError:
                    vals[mean_idx] = ""
            if mode_idx is not None:
                c = defaultdict(int)
                for i in xrange(len(items[mode_idx])):
                    if count_idx is not None:
                        this_count = int(items[count_idx][i])
                    else:
                        this_count = 1
                    c[items[mode_idx][i]] += this_count
                v = list(c.values())
                k = list(c.keys())
                vals[mode_idx] = k[v.index(max(v))]
            if len(percentile_idcs) > 0:
                # in lieu of a better approx. take ~median of each percentile
                # TODO: weight by count
                for idx in percentile_idcs:
                    num_vals = [float(x) for x in items[idx] if x not in 
                                ("", "nan")]
                    if len(num_vals) > 0:
                        vals[idx] = sorted(num_vals)[len(num_vals) / 2]
                    else:
                        vals[idx] = ""
            rec = delim.join((rec, delim.join(str(x) for x in vals)))
        print(rec)


def main():
    """ Point of code entry. """
    parser = prep_arg_parser()
    args = parser.parse_args()
    if len(args.merge) == 0 and args.field is None:
        parser.error("at least one of -f or -R is required")
    if len(args.merge) > 0:
        # merge mode instead
        merge_tables(args.merge, args.delim, args.printevery)
        sys.exit(0)
    d = defaultdict(StatList)
    recnum = 0
    args.backoff = [x.split(',')[1] if ',' in x else None for x in args.key
                    if x.split(',')[0] not in args.date]
    args.key = [x.split(',')[0] for x in args.key
                if x.split(',')[0] not in args.date]
    try:
        kf = KeyedFile(args.infile, args.field, args.key, args.backoff,
                       args.delim, args.header, args.date, args.lag,
                       args.width, args.alpha)
        for kr in kf.read_recs():
            recnum += 1
            for ks in kf.all_keysets(kr):
                d[ks].add(kr)
            if args.printevery > 0 and recnum % args.printevery == 0:
                sys.stderr.write("rec: %s\tunique_keys: %s\r" % (
                                 locale.format("%d", recnum, grouping=True),
                                 locale.format("%d", len(d), grouping=True)))
        sys.stderr.write('\n')
        out_hdr = args.delim.join(args.key)
        if len(args.date) > 0:
            out_hdr += args.delim + args.delim.join(args.date)
        if args.count:
            out_hdr += args.delim + args.field + "_count"
        if args.min:
            out_hdr += args.delim + args.field + "_min"
        if args.max:
            out_hdr += args.delim + args.field + "_max"
        if args.mean:
            out_hdr += args.delim + args.field + "_mean"
        if args.mode:
            out_hdr += args.delim + args.field + "_mode"
        if len(args.percentile) > 0:
            out_hdr += args.delim + args.delim.join([args.field +
                                                     "_percentile_" + str(x)
                                                     for x in
                                                     args.percentile])
        print out_hdr
        pool = Pool(processes=args.cores)
        print "\n".join(pool.map(calc_stats, [(d[k], k, args) for k in
                        sorted(d.iterkeys())]))
    except EmptyStdinError:
        print("warning: no files specified and nothing waiting at stdin")
        parser.print_help()
        sys.exit(1)
    except FieldNotFoundError as f:
        print("warning: invalid field name/offset specified: " + str(f))
        parser.print_help()
        sys.exit(2)


if __name__ == '__main__':
    main()
