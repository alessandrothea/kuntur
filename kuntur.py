#!/usr/bin/env python

import logging
import argparse
import sys
import os
import re
import subprocess
import tarfile
import os.path as path

from distutils.dir_util import mkpath
# from ktools import JidInserter, CheckExt, SplitList, SysArgVSentry


# -----------------------------------------------------------------------------
class JidInserter(object):
    """Generic utility class to add job id to filenames"""

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
    '''Utility class to enforce specific extensions on argparse arguments
    '''

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
class Job(object):
    '''Plain class to hold job attributes
    '''
    pass
# -----------------------------------------------------------------------------

# Condor job flavours
# espresso     = 20 minutes
# microcentury = 1 hour
# longlunch    = 2 hours
# workday      = 8 hours
# tomorrow     = 1 day
# testmatch    = 3 days
# nextweek     = 1 week


# -----------------------------------------------------------------------------
class ArgParser(object):
    '''Helper class that takes care of command line argument parsing
    '''

    flavours = [
        'espresso',
        'microcentury',
        'longlunch',
        'workday',
        'tomorrow',
        'testmatch',
        'nextweek',
    ]

    def __init__(self, argv=sys.argv[1:]):
        super(ArgParser, self).__init__()

        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument('-n', dest='dryRun', action='store_true')
        parser.add_argument('-j', dest='nJobs', type=int, default=1)
        parser.add_argument('-f', dest='flavour', choices=self.flavours, default=None)
        parser.add_argument('cmsswCfgPath', metavar='CMSSWCFG', action=CheckExt, choiches=['py'])
        parser.add_argument('cmsswArgs', metavar='CMSSWARGS', nargs=argparse.REMAINDER)
        self.args = vars(parser.parse_args(argv))

        self.args['cmsswBase'] = os.environ['CMSSW_BASE']
        self.args['cmsswVersion'] = os.environ['CMSSW_VERSION']
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
class Kuntur(object):
    '''Simple class to mitigate the pain of processing a CMSSW dataset on condor by splitting it into multiple jobs

    Args
    ----

    dryRun: bool
        Do not submit jobs if True

    nJobs: int
        Number of jobs to submit

    flavour: string
        Flavour cof the conder job

    cmsswCfgPath: string
        CMSSW configuration file

    cmsswArgs: list(string)
        Arguments of the CMSSW configuration file

    cmsswBase: string
        Location of the CMSSW sandbox

    cmsswVersion: string
        CMSSW version
    '''

    log = logging.getLogger('Kuntur')

    workPrefix = 'work'
    tarballname = 'cmsswpod.tgz'
    scriptname = 'kundur_worker'
    condorcardname = 'condor.card'
    jobname = 'job.log'
    outname = 'out.txt'
    errname = 'err.txt'

    scripttmpl = '''
#!/bin/bash

# A nice opening message
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
'''


    # -----------------------------------------------------------------------------
    def __init__(self, **kwargs):
        super(Kuntur, self).__init__()
        self.log.info('kutur created')

        # Adopt the arguments as data members
        self.__dict__.update(**kwargs)

        for k, a in kwargs.iteritems():
            logging.info(" - %s: %s", k, a)
    # -----------------------------------------------------------------------------
    
    # -----------------------------------------------------------------------------
    @property
    def condortmpl(self):
        '''
        # Send afs credentials
        # send_credential = True

        # Use proxy? Not sure how it works
        # Use_X509UserProxy = True
        # X509UserProxy = <Local proxy file>
        '''
        tmpl = '''
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
'''

        if self.flavour:
            tmpl += '''
+JobFlavour = "{flavour}"
'''

        tmpl += '''
# No. of jobs to submit (N.B. no equals sign)
Queue 1
'''
        return tmpl
    # -----------------------------------------------------------------------------
    

    # -----------------------------------------------------------------------------
    def prepare(self):
        '''Takes care of the preliminary paperworks, define directories, names, etc
        '''

        # Original CMSSW config file name
        self.cfgfilepath = path.normpath(path.expandvars(self.cmsswCfgPath))

        if not path.exists(self.cfgfilepath):
            raise RuntimeError("No such file or directory: '"+self.cfgfilepath+"'")

        # Extact config file name
        self.cfgfullname = path.splitext(path.basename(self.cfgfilepath))[0]

        # Remove _cfg postfix if exists
        self.cfgname = re.sub('_cfg$', '', self.cfgfullname)

        # Name of the new working area
        # workdir = '{0}_{1}_{2}'.format(
        #     workPrefix,
        #     cfgname,
        #     time.strftime('%Y%m%d_%H%M%S')
        # )
        workdir = path.join(
            os.getcwd(),
            '{0}_{1}'.format(
                self.workPrefix,
                self.cfgname
            )
        )

        workdir = path.join(os.getcwd(), self.workPrefix)
        indir = path.join(workdir, 'ins')
        outdir = path.join(workdir, 'out')
        logdir = path.join(workdir, 'log')

        self.workdir = workdir
        self.indir = indir
        self.outdir = outdir
        self.logdir = logdir

        # Define a bunch of useful paths
        self.scriptpath = path.join(workdir, self.scriptname+'.sh')
        self.tarballpath = path.join(workdir, self.tarballname)
        self.cfgdumppath = path.join(indir, self.cfgname+'_dump.py')
        self.condorcardpath = path.join(indir, self.condorcardname)

        self.logpath = path.join(logdir, self.jobname)
        self.outpath = path.join(logdir, self.outname)
        self.errpath = path.join(logdir, self.errname)
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def setup(self):
        '''Clean up the old directories and create new ones
        '''

        # Delete the old directory
        subprocess.call('rm -rf {0}'.format(self.workdir), shell=True)

        self.log.info('+ Creating work area')
        # Create all required directories
        mkpath(self.workdir)
        mkpath(self.indir)
        mkpath(self.outdir)
        mkpath(self.logdir)
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def loadCMSSWConfig(self):
        '''Loads CMSSW configuration file and stores it for later'''
        import FWCore.ParameterSet.Config as cms

        cfgArgV = [self.cmsswCfgPath]+self.cmsswArgs

        self.log.info("+ Loading CMSSW configuration")
        process = None
        # Import the config file, applying the right args
        with SysArgVSentry(cfgArgV) as argvSentry:
            cfgGlobals = {}
            execfile(self.cfgfilepath, cfgGlobals)

            # Extract the process
            # Is process a generic name? likely so, CMSSE needs to know what object to process
            try:
                process = cfgGlobals['process']
            except KeyError:
                raise RuntimeError('CMSSW process not found in config file '+self.cfgfilepath)
        self.log.info("  Configuration loading completed")

        self.process = process
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def splitCMSSWJob(self):
        import FWCore.ParameterSet.Config as cms

        # Local reference
        process = self.process

        jobs = {}

        if len(process.source.secondaryFileNames):
            raise RuntimeError('No support for secondaryFilenames yet, sorry')

        if len(process.source.fileNames) < self.nJobs:
            raise RuntimeError('Cannot split {0} files on {1} jobs'.format(len(process.source.fileNames), self.nJobs))

        inserter = JidInserter(self.nJobs)

        # Split the inputfiles in nJobs groups
        inputFiles = SplitList(process.source.fileNames, self.nJobs)

        # Save original list of output files
        # For TFileService objects
        tfileServices = {n: s.fileName for n, s in process.services.iteritems() if s.type_() == 'TFileService'}
        outputModules = {n: s.fileName for n, s in process.outputModules.iteritems() if s.type_() == 'PoolOutputModule'}

        for j in xrange(self.nJobs):
            # Update process input files
            process.source.fileNames = cms.untracked.vstring(inputFiles[j])
            process.source.fileNames.setIsTracked(False)

            for n, fileName in tfileServices.iteritems():
                fileService = getattr(process, n)
                fileService.fileName = inserter(fileName, j)

            for n, fileName in outputModules.iteritems():
                outputModule = getattr(process, n)
                outputModule.fileName = inserter(fileName, j)

            # jobs[inserter(cfgfilename, j)] = process.dumpPython()
            # jobs[j] = {
            #     'nfiles': len(process.source.fileNames),
            #     'cfgpath': inserter(self.cfgdumppath, j),
            #     'cfg': process.dumpPython(),
            # }
            job = Job()
            job.nfiles = len(process.source.fileNames)
            job.cfgpath = inserter(self.cfgdumppath, j)
            job.cfg = process.dumpPython()
            job.logpath = inserter(self.logpath, j)
            job.outpath = inserter(self.outpath, j)
            job.errpath = inserter(self.errpath, j)
            job.condorcardpath = inserter(self.condorcardpath, j)
            jobs[j] = job

        self.jobs = jobs
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def writeWorkerScript(self):

        # Local reference
        # cmsswVersion = self.cfgSplitRcpt['cmsswVersion']
        # cmsswBase    = self.cfgSplitRcpt['cmsswBase']

        self.log.info('+ Generating worker script')
        script = self.scripttmpl.format(**vars(self))

        with open(self.scriptpath, 'w') as scriptfile:
            scriptfile.write(script)

        os.chmod(self.scriptpath, 0755)
        self.log.info('  Worker script: %s', self.scriptpath)
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def writeCondorCards(self):
        # print self.condortmpl
        self.log.info('+ Generating job condor cards and CMSSW configs')
        for i, job in self.jobs.iteritems():

            # Compose dictionaly for filling the condor template
            pars = vars(self)
            pars.update(vars(job))

            self.log.info('  %d | CMSSW cfg: %s', i, job.cfgpath)
            with open(job.cfgpath, 'w') as cfgfile:
                cfgfile.write(job.cfg)

            self.log.info('  %d | Condor card: %s', i, job.condorcardpath)
            with open(job.condorcardpath, 'w') as condorfile:
                condorfile.write(self.condortmpl.format(**pars))
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def tarCMSSWOld(self):

        self.log.info("+ Creating CMSSW tarball ")
        zipdirs = [d for d in ['bin', 'lib', 'python', 'data'] if path.exists(path.join(self.cmsswBase, d))]
        # Zip the good stuff
        subprocess.check_call(['tar', 'cvfz', self.tarballpath, '--directory='+self.cmsswBase] + zipdirs)
        self.log.info('  tarball: %s', self.tarballpath)
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def tarCMSSW(self):
        self.log.info("+ Creating CMSSW tarball ")
        cmsswdirs = ['bin', 'biglib', 'lib', 'python', 'data']
        with tarfile.open(self.tarballpath, mode="w:gz", dereference=True) as tarball:
            for d in cmsswdirs:
                dirPath = path.join(self.cmsswBase, d)

                if not path.exists(dirPath):
                    continue
                self.log.info('  Adding %s', repr(d))
                tarball.add(dirPath, d, recursive=True)
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def submitJobs(self):
        self.log.info('+ Submitting condor jobs')
        for i, job in self.jobs.iteritems():
            subprocess.check_call(['condor_submit', job.condorcardpath])

        self.log.info('+ Condor queue status')
        subprocess.check_call(['condor_q'])
    # -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------


if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)-8s: %(message)s', level=logging.INFO)

    parser = ArgParser()

    ktr = Kuntur(**(parser.args))
    ktr.prepare()
    ktr.setup()
    ktr.loadCMSSWConfig()
    ktr.tarCMSSW()
    ktr.writeWorkerScript()
    ktr.splitCMSSWJob()
    ktr.writeCondorCards()
    if not parser.args['dryRun']:
        ktr.submitJobs()
