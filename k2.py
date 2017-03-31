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
import argparse
import re
import subprocess
import json

from distutils.dir_util import mkpath
from ktools import JidInserter, CheckExt


class Kuntur(object):
    """docstring for Kuntur"""

    workPrefix = 'work'
    tarballname = 'cmsswpod.tgz'
    condorname = 'condor.card'
    jobname = 'job.log'
    outname = 'out.txt'
    errname = 'err.txt'

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
'''

    def __init__(self):
        super(Kuntur, self).__init__()

    # -----------------------------------------------------------------------------
    def parse(self, argv=sys.argv[1:]):
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
        parser.add_argument('cmsswCfg', metavar='CMSSWCFG', action=CheckExt, choiches=['py'])
        parser.add_argument('cmsswArgs', metavar='CMSSWARGS', nargs=argparse.REMAINDER)
        self.args = parser.parse_args(argv)

        print self.args
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def setup(self):

        # Local reference
        args = self.args

        cfgfilepath = path.normpath(path.expandvars(args.cmsswCfg))

        # Validate arguments
        # Ensure that the CMSSW config file exists
        if not path.exists(cfgfilepath):
            raise RuntimeError("No such file or directory: '"+cfgfilepath+"'")

        # Extact config file name
        self.cfgfullname = path.splitext(path.basename(cfgfilepath))[0]

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
        self.scriptpath = path.join(workdir, self.cfgname+'.sh')
        self.tarballpath = path.join(workdir, self.tarballname)
        self.cfgdumppath = path.join(indir, self.cfgname+'_dump.py')
        self.condorpath = path.join(indir, self.condorname)

        self.logpath = path.join(logdir, self.jobname)
        self.outpath = path.join(logdir, self.outname)
        self.errpath = path.join(logdir, self.errname)

        # Delete the old directory
        subprocess.call('rm -rf {0}'.format(workdir), shell=True)

        # Create all required directories
        mkpath(workdir)
        mkpath(indir)
        mkpath(outdir)
        mkpath(logdir)
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def makeCMSSWConfigs(self):

        # Local reference
        args = self.args
        indir = self.indir
        cfgfullname = self.cfgfullname

        # Process the CMSSW configuration
        edmCfgDumpPath = path.join(path.dirname(__file__), 'edmCfgSplitter.py')
        cfgArgV = [args.cmsswCfg]+args.cmsswArgs

        cmd = [
            # 'echo',
            edmCfgDumpPath,
            '-o', indir,
            str(args.nJobs)
        ] + cfgArgV

        print '* Generating CMSSW config files'
        subprocess.check_call(' '.join(cmd), shell=True)
        print '* Done'

        # Load the description of the split configs
        cfgSplitRcpt = None
        with open(path.join(self.indir, cfgfullname+'.json')) as j:
            cfgSplitRcpt = json.loads(j.read())

        self.cfgSplitRcpt = cfgSplitRcpt
        print '* Job descriptions loaded'

    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def makeScript(self):

        # Local reference
        cmsswVersion = self.cfgSplitRcpt['cmsswVersion']
        # cmsswBase    = self.cfgSplitRcpt['cmsswBase']

        print '* Generating job script'
        script = self.script.format(**{
            'tarballpath': self.tarballpath,
            'outdir': self.outdir,
            'cmsswVersion': cmsswVersion,
        })

        with open(self.scriptpath, 'w') as scriptfile:
            scriptfile.write(script)

        os.chmod(self.scriptpath, 0755)
        print '* Done'
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def tarCMSSW(self):

        # Local reference
        cmsswBase = self.cfgSplitRcpt['cmsswBase']

        print "* Creating CMSSW tarball", self.tarballpath
        zipdirs = [d for d in ['bin', 'lib', 'python', 'data'] if path.exists(path.join(cmsswBase, d))]
        # Zip the good stuff
        subprocess.check_call(['tar', 'cvfz', self.tarballpath, '--directory='+cmsswBase] + zipdirs)
        print '* Done'
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def writeCondorCards(self):
        # Prepare the condor datacard
        condorVariables = {
            "scriptpath": self.scriptpath
        }

        # this is the card template
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

        if self.args.jobFlavour:
            condorTemplate += '''
        +JobFlavour = "{jobFlavour}"
        '''
            condorVariables.update({
                'jobFlavour': self.args.jobFlavour
            })

        condorTemplate += '''
        # No. of jobs to submit (N.B. no equals sign)
        Queue 1
        '''

        # Writing condor cards
        inserter = JidInserter(self.args.nJobs)

        print '* Generating Condor cards'
        for jid, jdesc in self.cfgSplitRcpt['jobs'].iteritems():
            j = int(jid)
            jobVars = condorVariables.copy()
            jobVars['cfgpath'] = jdesc['cfgpath']
            jobVars['logpath'] = inserter(self.logpath, j)
            jobVars['outpath'] = inserter(self.outpath, j)
            jobVars['errpath'] = inserter(self.errpath, j)

            card = condorTemplate.format(**jobVars)
            jdesc['condorcard'] = inserter(self.condorpath, j)

            with open(jdesc['condorcard'], 'w') as condorfile:
                condorfile.write(card)
        print '* Done'
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def submitJobs(self):
        for jid, jdesc in self.cfgSplitRcpt['jobs'].iteritems():
            subprocess.check_call(['condor_submit', jdesc['condorcard']])


# -----------------------------------------------------------------------------
if __name__ == '__main__':
    ku = Kuntur()
    ku.parse()
    ku.setup()
    ku.makeCMSSWConfigs()
    ku.makeScript()
    ku.tarCMSSW()
    ku.writeCondorCards()
    ku.submitJobs()
