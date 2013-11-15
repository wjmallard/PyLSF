PyLSF
=====

PyLSF is a Python wrapper for a tiny subset of LSF commands.
While far from comprehensive,
it provides access to the most commonly used
job management tools on an LSF cluster:
submit, status, and kill.

PyLSF is intended to serve as a reference for other scientists wishing to
automate job submission and management from within computational pipelines.
It is more robust than, say, calling ```bsub``` with ```subprocess.call()```.

Since PyLSF uses the native Python API,
a full wrapper for the LSF API is infeasible.
For a comprehensive LSF wrapper,
check out https://github.com/gmccance/pylsf
which uses Pyrex to automate much of the work.

The PyLSF API includes the following functions.

```python
submit(command, jobName=None, queue=None, processors=1, memory=0, resReq=None, stdout=None, stderr=None) -> int
    Submit an LSF job.
    Returns a jobId.

status(jobId) -> int
    Return the status of an LSF job.

wait(jobId)
    Wait for an LSF job to complete.

kill(jobId)
    Kill an LSF job.

batch_status(jobName) -> int
    Return the number of incomplete jobs in an LSF batch.

batch_wait(jobName)
    Wait for a batch of LSF jobs to complete.

batch_kill(jobName)
    Kill a batch of LSF jobs.
```

