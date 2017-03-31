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
        with open(path.join(indir, cfgfullname+'.json')) as j:
            cfgSplitRcpt = json.loads(j.read())

        cmsswVersion = cfgSplitRcpt['cmsswVersion']
        cmsswBase    = cfgSplitRcpt['cmsswBase']
    # -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    ku = Kuntur()
    ku.parse()
    ku.setup()
    ku.makeCMSSWConfigs()

if False:
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
    cfgfullname = path.splitext(path.basename(cfgfilepath))[0]
    # Remove _cfg postfix if exists
    cfgname = re.sub('_cfg$', '', cfgfullname)

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
    with open(path.join(indir, cfgfullname+'.json')) as j:
        cfgSplitRcpt = json.loads(j.read())

    cmsswVersion = cfgSplitRcpt['cmsswVersion']
    cmsswBase    = cfgSplitRcpt['cmsswBase']
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

    '''.format(**{
        'tarballpath': tarballpath,
        'outdir': outdir,
        'cmsswVersion': cmsswVersion,
        })

    with open(scriptpath, 'w') as scriptfile:
        scriptfile.write(script)

    os.chmod(scriptpath, 0755)
    # -----------------------------------------------------------------------------


    # -----------------------------------------------------------------------------
    # Prepare the condor datacard
    condorVariables = {
        "scriptpath": scriptpath
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


    # -----------------------------------------------------------------------------
    # Writing condor cards
    inserter = JidInserter(args.nJobs)

    print '* Generating Condor cards'
    for j,k in cfgSplitRcpt['jobs'].iteritems():
        j = int(j)
        jobVars = condorVariables.copy()
        jobVars['cfgpath'] = k['cfgpath']
        jobVars['logpath'] = inserter(logpath, j)
        jobVars['outpath'] = inserter(outpath, j)
        jobVars['errpath'] = inserter(errpath, j)

        card = condorTemplate.format(**jobVars)
        with open(inserter(condorpath, j), 'w') as condorfile:
            condorfile.write(card)
    print '* Done'
    # -----------------------------------------------------------------------------


    # -----------------------------------------------------------------------------
    if not args.dryRun:
        print "* Creating CMSSW tarball", tarballpath
        zipdirs = [ d for d in ['bin', 'lib', 'python', 'data'] if path.exists(path.join(cmsswBase, d))]
        # Zip the good stuff
        subprocess.check_call(['tar', 'cvfz', tarballpath, '--directory='+cmsswBase] + zipdirs)
    # -----------------------------------------------------------------------------