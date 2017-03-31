#!/usr/bin/python

import os
import sys
import subprocess
import htcondor

print __file__, sys.version_info

print os.environ['CMSSW_BASE']

preCmd = "cd ${CMSSW_BASE}; eval `scramv1 runtime -sh`;"
subprocess.check_call(preCmd + 'krunned.py', shell=True)