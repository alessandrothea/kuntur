import htcondor

coll = htcondor.Collector()
ads = coll.locateAll(htcondor.DaemonTypes.Schedd)
for ad in ads:
    print ad["Name"]

schedd = htcondor.Schedd()
jobid = schedd.submit({"Cmd": "/bin/echo"})
print repr(jobid)


contraint = 'JobId =?= {0}'.format(jobid)

job_ad = schedd.query(contraint)
print 'ad = ', job_ad

schedd.act(htcondor.JobAction.Remove, [str(jobid)])
