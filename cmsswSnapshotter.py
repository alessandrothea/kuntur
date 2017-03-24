#!/usr/bin/env python

import argparse
import sys
import os
import os.path as path
import json
import FWCore.ParameterSet.Config as cms


# -----------------------------------------------------------------------------
def SplitList(lst, n):
    cls = lst.__class__
    # Group size (approximate)
    gs = len(lst) / n

    # Reminder
    rmndr = len(lst) % n

    grps = []
    for i in xrange(n):

        # Calculate the indexes
        if i < rmndr:
            # Groups with index lower than reminder get an additional element
            j, s = (i*(gs+1), gs+1)
        else:
            # Groups beyond have standard size
            j, s = (rmndr*(gs+1)+(i-rmndr)*gs, gs)

        # New
        grp = cls(lst[j:j+s])
        # print i, j, len(grp)
        grps += [grp]

    return grps
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
class JidInserter(object):
    """docstring for JidInserter"""

    def __init__(self, njobs):
        super(JidInserter, self).__init__()
        self._njobs = njobs
        self._fmt = '{{}}_{{:0{}d}}{{}}'.format(len(str(njobs)))

    def __call__(self, fileName, i):
        fileString = fileName if isinstance(fileName, str) else fileName.value()
        name, ext = path.splitext(fileString)
        return fileName.__class__(self._fmt.format(name, i, ext))
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
class CheckExt(argparse.Action):

    def __init__(self, *args, **kwargs):
        # if 'choiches' in kwargs:
        self._choices = kwargs.pop('choiches')
        super(CheckExt, self).__init__(*args, **kwargs)
        # self._choices = choiches

    def __call__(self, parser, namespace, fname, option_string=None):
        ext = path.splitext(fname)[1][1:]
        if ext not in self._choices:
            option_string = '({})'.format(option_string) if option_string else ''
            parser.error("file doesn't end with one of {}{}".format(self._choices, option_string))
        else:
            setattr(namespace, self.dest, fname)
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
class SysArgVSentry(object):
    """RAII pattern to temporaryly modify command arguments
    """
    def __init__(self, argv):
        super(SysArgVSentry, self).__init__()
        self.argv = argv

    def __enter__(self):
        self._oldargv = sys.argv[:]
        sys.argv = self.argv
        return self

    def __exit__(self, type, value, traceback):
        sys.argv = self._oldargv
# -----------------------------------------------------------------------------


parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('jobs', type=int, default=1)
parser.add_argument('cmsswCfg', metavar='CMSSWCFG', action=CheckExt, choiches=['py'])
parser.add_argument('cmsswArgs', metavar='CMSSWARGS', nargs=argparse.REMAINDER)
args = parser.parse_args()


# -----------------------------------------------------------------------------
# Check environment
cfgfilepath = path.normpath(path.expandvars(args.cmsswCfg))
cfgfilename = path.basename(cfgfilepath)
splitrcpt = path.splitext(cfgfilename)[0] + '.json'

cmsswBase = os.environ['CMSSW_BASE']
cmsswVersion = os.environ['CMSSW_VERSION']
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Prepare a standalone CMSSW config
cfgArgV = [args.cmsswCfg]+args.cmsswArgs

print "--- Parsing CMSSW configuration:", repr(args.cmsswCfg)
process = None
# Import the config file, applying the right args
with SysArgVSentry(cfgArgV) as argvSentry:
    cfgGlobals = {}
    execfile(cfgfilepath, cfgGlobals)

    # Extract the process
    # Is process a generic name? likely so, CMSSE needs to know what object to process
    try:
        process = cfgGlobals['process']
    except KeyError as ke:
        raise RuntimeError('CMSSW process not found in config file '+cfgfilepath)
# -----------------------------------------------------------------------------

configurations = {}

# -----------------------------------------------------------------------------
if args.jobs == 1:
    configurations[0] = {
        'jid': 0,
        'cfgfile': cfgfilename,
        'nfiles': len(process.source.fileNames),
        'cfg': process.dumpPython()
    }

# -----------------------------------------------------------------------------
#
else:
    if len(process.source.secondaryFileNames):
        raise RuntimeError('No support for secondaryFilenames yet, sorry')

    if len(process.source.fileNames) < args.jobs:
        raise RuntimeError('Cannot split {0} files on {1} jobs'.format(len(process.source.fileNames), args.jobs))

    inserter = JidInserter(args.jobs)

    # Split the inputfiles in nJobs groups
    inputFiles = SplitList(process.source.fileNames, args.jobs)

    # Save original list of output files
    # For TFileService objects
    tfileServices = {n: s.fileName for n, s in process.services.iteritems() if s.type_() == 'TFileService'}
    outputModules = {n: s.fileName for n, s in process.outputModules.iteritems() if s.type_() == 'PoolOutputModule'}

    for j in xrange(args.jobs):
        # Update process input files
        process.source.fileNames = cms.untracked.vstring(inputFiles[j])
        process.source.fileNames.setIsTracked(False)

        for n, fileName in tfileServices.iteritems():
            fileService = getattr(process, n)
            fileService.fileName = inserter(fileName, j)

        for n, fileName in outputModules.iteritems():
            outputModule = getattr(process, n)
            outputModule.fileName = inserter(fileName, j)

        # configurations[inserter(cfgfilename, j)] = process.dumpPython()
        configurations[j] = {
            'jid': j,
            'cfgfile': inserter(cfgfilename, j),
            'nfiles': len(process.source.fileNames),
            'cfg': process.dumpPython()
        }

# Save the newly made configurations to file
for j, cfgData in configurations.iteritems():
    with open(cfgData['cfgfile'], 'w') as cfgFile:
        cfgFile.write(cfgData['cfg'])

# Prepare the receipt
jsonreceipt = configurations.copy()
map(lambda j: j[1].pop('cfg'), jsonreceipt.iteritems())

# Save the receipt to file
with open(splitrcpt, 'w') as receipt:
    receipt.write(json.dumps(jsonreceipt, sort_keys=True, indent=4, separators=(',', ': ')))
