#!/usr/bin/env python

# Condor job flavours
# espresso     = 20 minutes
# microcentury = 1 hour
# longlunch    = 2 hours
# workday      = 8 hours
# tomorrow     = 1 day
# testmatch    = 3 days
# nextweek     = 1 week

import sys
import os
import os.path as path
import re
import subprocess
import argparse
from distutils.dir_util import mkpath


class Job(object):
    """docstring for Job"""
    def __init__(self, jid):
        super(Job, self).__init__()
        self.jid = jid
        self.cfgdump = None
        self.cfgpath = None


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


condorJobFlavours = [
    'espresso',
    'microcentury',
    'longlunch',
    'workday',
    'tomorrow',
    'testmatch',
    'nextweek',
]

# Try parsing, unclear if this is going to work
# kuntur <kuntur opts> cfg.py <cfg opts but no 'dashes'>
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-n', dest='dryRun', action='store_true')
parser.add_argument('-j', dest='nJobs', type=int, default=1)
parser.add_argument('-f', dest='jobFlavour', choices=condorJobFlavours, default=None)
parser.add_argument('cmsswCfg', metavar='CMSSWCFG')
parser.add_argument('cmsswArgs', metavar='CMSSWARGS', nargs=argparse.REMAINDER)
args = parser.parse_args()

# ----
tarball = 'cmsswpod.tgz'
workPrefix = 'work'
cfgfilepath = path.normpath(path.expandvars(args.cmsswCfg))
# ----

# -----------------------------------------------------------------------------
# Validate arguments
# Ensure that the CMSSW config file exists
if not path.exists(cfgfilepath):
    raise RuntimeError("No such file or directory: '"+cfgfilepath+"'")

# -----------------------------------------------------------------------------
# Extact config file name
cfgname = path.splitext(path.basename(cfgfilepath))[0]
# Remove _cfg postfix if exists
cfgname = re.sub('_cfg$', '', cfgname)

# Name of the new working area
# workdir = '{0}_{1}_{2}'.format(
#     workPrefix,
#     cfgname,
#     time.strftime('%Y%m%d_%H%M%S')
# )
workdir = path.join(
    os.getcwd(),
    '{0}_{1}'.format(
        workPrefix,
        cfgname
    )
)
workdir = path.join(os.getcwd(), workPrefix)
indir = path.join(workdir, 'ins')
outdir = path.join(workdir, 'out')
logdir = path.join(workdir, 'log')

# Define a bunch of useful paths
scriptpath = path.join(workdir, cfgname+'.sh')
tarballpath = path.join(workdir, tarball)
cfgdumppath = path.join(indir, cfgname+'_dump.py')
condorpath = path.join(indir, 'condor.card')
logpath = path.join(logdir, 'job.log')
outpath = path.join(logdir, 'out.txt')
errpath = path.join(logdir, 'err.txt')
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# Delete the old directory
subprocess.call('rm -rf {0}'.format(workdir), shell=True)

# Create all required directories
mkpath(workdir)
mkpath(indir)
mkpath(outdir)
mkpath(logdir)
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
cmsswBase = os.environ['CMSSW_BASE']
cmsswVersion = os.environ['CMSSW_VERSION']
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Prepare a standalone CMSSW config
print 'cmssw configuration:', args.cmsswCfg
print 'cmssw arguments:', args.cmsswArgs

cfgArgV = [args.cmsswCfg]+args.cmsswArgs

print " * Parsing CMSSW config", args.cmsswCfg
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


# -----------------------------------------------------------------------------
# Prepare the executor script
script = '''
#!/bin/bash

echo "Running '${{BASH_SOURCE}}'"

if [ "$#" -ne 1 ]; then
    echo "Usage ${{BASH_SOURCE}} <cmssw config>"
fi
CMSCFG=$1

echo "--- Working directory ${{PWD}} ---"

echo "Setting up CMSSW area {cmsswVersion}"

scram project CMSSW {cmsswVersion}

cd {cmsswVersion}

eval `scram runtime -sh`

tar xvfz {tarballpath}

cd {outdir}

cmsRun ${{CMSCFG}}

echo "FINISHED"

'''.format(**locals())

with open(scriptpath, 'w') as scriptfile:
    scriptfile.write(script)

os.chmod(scriptpath, 0755)
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
nJobs = args.nJobs

jobs = []
cfgFiles = {}
if nJobs == 1:

    job = Job(0)
    job.cfgpath = cfgdumppath
    job.cfgdump = process.dumpPython()
    job.logpath = logpath
    job.outpath = outpath
    job.errpath = errpath
    job.cardpath = condorpath

    jobs.append(job)
else:
    if len(process.source.secondaryFileNames):
        raise RuntimeError('No support for secondaryFilenames yet, sorry')

    if len(process.source.fileNames) < nJobs:
        raise RuntimeError('Cannot split {0} files on {1} jobs'.format(len(process.source.fileNames), nJobs))

    # Split the inputfiles in nJobs groups
    inputFiles = SplitList(process.source.fileNames, nJobs)

    # Save original list of output files
    fs = {n: s.fileName for n, s in process.services.iteritems() if s.type_() == 'TFileService'}
    om = {n: s.fileName for n, s in process.outputModules.iteritems() if s.type_() == 'PoolOutputModule'}

    def insertJidInCmsFile(fileName, i):
        fileString = fileName if isinstance(fileName, str) else fileName.value()
        name, ext = path.splitext(fileString)
        return fileName.__class__('%s_%03d%s' % (name, i, ext))

    for j in xrange(nJobs):
        import FWCore.ParameterSet.Config as cms

        # Update process files
        process.source.fileNames = cms.untracked.vstring(inputFiles[j])

        for n, fileName in fs.iteritems():
            fileService = getattr(process, n)
            fileService.fileName = insertJidInCmsFile(fileName, j)

        for n, fileName in om.iteritems():
            outputModule = getattr(process, n)
            outputModule.fileName = insertJidInCmsFile(fileName, j)

        # cfgFiles[insertJidInCmsFile(cfgdumppath, j)] = process.dumpPython()
        job = Job(j)
        job.cfgpath = insertJidInCmsFile(cfgdumppath, j)
        job.cfgdump = process.dumpPython()
        job.logpath = insertJidInCmsFile(logpath, j)
        job.outpath = insertJidInCmsFile(outpath, j)
        job.errpath = insertJidInCmsFile(errpath, j)
        job.cardpath = insertJidInCmsFile(condorpath, j)

        jobs.append(job)
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
condorVariables = {
    "scriptpath": scriptpath
}

# Prepare the condor datacard
condorTemplate = '''
###############################################
# Condor batch system configuration card file #
###############################################
# Warning - comments must be on different lines to functional statements.
Universe        = vanilla
Executable      = {scriptpath}
Arguments       = {cfgpath}

Log             = {logpath}
Output          = {outpath}
Error           = {errpath}

GetEnv          = True

# Send afs credentials
# send_credential = True

# Use proxy? Not sure how it works
# Use_X509UserProxy = True
# X509UserProxy = <Local proxy file>
'''

if args.jobFlavour:
    condorTemplate += '''
+JobFlavour = "{jobFlavour}"
'''
    condorVariables.update({
        'jobFlavour': args.jobFlavour
    })

condorTemplate += '''
# No. of jobs to submit (N.B. no equals sign)
Queue 1
'''

# -----------------------------------------------------------------------------
# Save configurations to file
for job in jobs:
    print "* Saving cmssw configuration", job.cfgpath
    # print job.cfgpath, len(job.cfgdump), job.logpath, job.outpath
    with open(job.cfgpath, 'w') as cfgFile:
        cfgFile.write(job.cfgdump)

    print "* Saving condor file", job.cardpath
    lv = condorVariables.copy()
    lv.update(vars(job))

    card = condorTemplate.format(**lv)
    with open(job.cardpath, 'w') as condorfile:
        condorfile.write(card)
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
if not args.dryRun:
    print "* Creating CMSSW tarball", tarballpath
    zipdirs = ['bin', 'lib', 'python']
    # Zip the good stuff
    subprocess.check_call(['tar', 'cvfz', tarballpath, '--directory='+cmsswBase] + zipdirs)
# -----------------------------------------------------------------------------
