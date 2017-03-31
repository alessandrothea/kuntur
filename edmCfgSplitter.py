#!/usr/bin/env python

import argparse
import sys
import os
import os.path as path
import json
import logging

from distutils.dir_util import mkpath
from ktools import JidInserter, CheckExt, SplitList, SysArgVSentry


# -----------------------------------------------------------------------------
class ConfigSplitter(object):
    '''Class to split a CMSSW configuration in nJobs sub-configurations
    '''

    _log = logging.getLogger('ConfigSplitter')

    @property
    def log(self):
        return self._log

    def __init__(self,):
        super(ConfigSplitter, self).__init__()

    # -----------------------------------------------------------------------------
    def parse(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument('-o', dest='output', default='.')
        parser.add_argument('jobs', type=int, default=1)
        parser.add_argument('cmsswCfg', metavar='CMSSWCFG', action=CheckExt, choiches=['py'])
        parser.add_argument('cmsswArgs', metavar='CMSSWARGS', nargs=argparse.REMAINDER)

        self.args = parser.parse_args()
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def setup(self):

        # Local reference
        args = self.args

        mkpath(args.output)

        # Check environment
        self.cfgfilepath = path.normpath(path.expandvars(args.cmsswCfg))
        self.cfgfilename = path.join(args.output, path.basename(self.cfgfilepath))
        self.recepit = path.splitext(self.cfgfilename)[0] + '.json'

        self.cmsswBase = os.environ['CMSSW_BASE']
        self.cmsswVersion = os.environ['CMSSW_VERSION']
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def loadCMSSWConfig(self):
        import FWCore.ParameterSet.Config as cms

        cfgArgV = [self.args.cmsswCfg]+self.args.cmsswArgs

        self.log.info("--- Loading CMSSW configuration")  # , repr(args.cmsswCfg)
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
        self.log.info("--- Configuration loading completed")

        self.process = process
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def splitCMSSW(self):
        import FWCore.ParameterSet.Config as cms

        # Local reference
        process = self.process
        args = self.args

        configurations = {}

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
                'cfgpath': inserter(self.cfgfilename, j),
                'nfiles': len(process.source.fileNames),
                'cfg': process.dumpPython()
            }

        self.configurations = configurations
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def save(self):
        '''
        Save new configurations and receipt on disk
        '''
        # Local references
        configurations = self.configurations

        # Save the newly made configurations to file
        self.log.info("--- Saving config files")
        for j, cfgData in configurations.iteritems():
            self.log.debug('    '+cfgData['cfgpath'])
            with open(cfgData['cfgpath'], 'w') as cfgFile:
                cfgFile.write(cfgData['cfg'])

        # Prepare the receipt for consumers of the new configurations
        jsonreceipt = {}

        # add cmssw version
        jsonreceipt['cmsswVersion'] = self.cmsswVersion
        jsonreceipt['cmsswBase'] = self.cmsswBase

        # add jobs description
        jobs = configurations.copy()
        map(lambda j: j[1].pop('cfg'), jobs.iteritems())

        jsonreceipt['jobs'] = jobs

        # Save the receipt to file
        self.log.info("--- Saving recepit")
        with open(self.recepit, 'w') as receipt:
            receipt.write(json.dumps(jsonreceipt, sort_keys=True, indent=4, separators=(',', ': ')))
    # -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)

    splitter = ConfigSplitter()
    splitter.parse()
    splitter.setup()
    splitter.loadCMSSWConfig()
    splitter.splitCMSSW()
    splitter.save()
# -----------------------------------------------------------------------------

