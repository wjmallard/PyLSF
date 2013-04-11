#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <Python.h>
#include <lsf/lsbatch.h>
#include <lsf/lsf.h>

#define POLLING_INTERVAL 5 /* seconds */

int lsf_submit(const char *, const char *, const char *, const char *, const char *);
int lsf_status(int);
int lsf_wait(int);
int lsf_kill(int);

static PyObject *PyLSF_submit(PyObject *, PyObject *, PyObject *);
static PyObject *PyLSF_status(PyObject *, PyObject *);
static PyObject *PyLSF_wait(PyObject *, PyObject *);
static PyObject *PyLSF_kill(PyObject *, PyObject *);

/*
 * LSF interface functions.
 */

int
lsf_submit(command, jobName, queue, stdout, stderr)
	const char *command;
	const char *jobName;
	const char *queue;
	const char *stdout;
	const char *stderr;
{
	int result;
	struct submit req;
	struct submitReply reply;
	LS_LONG_INT jobId;
	int i;

	/*
	 * Initialize LSF connection.
	 */
	result = lsb_init("PyLSF");

	if (result < 0)
	{
		lsb_perror("lsb_init() failed.");
		return(-1);
	}

	/*
	 * Prepare LSF submit request.
	 */
	memset(&req, 0, sizeof(struct submit));

	req.command = (char *)command;

	if (jobName != NULL)
	{
		req.jobName = (char *)jobName;
		req.options |= SUB_JOB_NAME;
	}

	if (queue != NULL)
	{
		req.queue = (char *)queue;
		req.options |= SUB_QUEUE;
	}

	if (stdout != NULL)
	{
		req.outFile = (char *)stdout;
		req.options |= SUB_OUT_FILE;
	}

	if (stderr != NULL)
	{
		req.errFile = (char *)stderr;
		req.options |= SUB_ERR_FILE;
	}

	req.numProcessors = 1;
	req.maxNumProcessors = 1;

	for (i = 0; i < LSF_RLIM_NLIMITS; i++)
	{
		req.rLimits[i] = DEFAULT_RLIMIT;
	}

	/*
	 * Send LSF submit request.
	 */
	jobId = lsb_submit(&req, &reply);

	if (jobId < 0)
	{
		switch lsberrno
		{
			case LSBE_QUEUE_USE:
			case LSBE_QUEUE_CLOSED:
				lsb_perror(reply.queue);
				return (int)jobId;
			default:
				lsb_perror(NULL);
				return (int)jobId;
		}
	}

	return (int)jobId;
}

int
lsf_status(jobId)
	int jobId;
{
	int numJobs;
	struct jobInfoEnt *job;
	int numJobsRemaining;
	int jobStatus;

	numJobs = lsb_openjobinfo(jobId, NULL, NULL, NULL, NULL, ALL_JOB);
	if (numJobs < 0)
	{
		lsb_perror("lsb_openjobinfo");
		return numJobs;
	}

	job = lsb_readjobinfo(&numJobsRemaining);
	if (job == NULL)
	{
		lsb_perror("lsb_readjobinfo");
		return -1;
	}

	lsb_closejobinfo();

	/*
	 * JOB_STAT_NULL   0x00000 (0)
	 * JOB_STAT_PEND   0x00001 (1)
	 * JOB_STAT_PSUSP  0x00002 (2)
	 * JOB_STAT_RUN    0x00004 (4)
	 * JOB_STAT_SSUSP  0x00008 (8)
	 * JOB_STAT_USUSP  0x00010 (16)
	 * JOB_STAT_EXIT   0x00020 (32)
	 * JOB_STAT_DONE   0x00040 (64)
	 * JOB_STAT_PDONE  0x00080 (128)
	 * JOB_STAT_PERR   0x00100 (256)
	 * JOB_STAT_WAIT   0x00200 (512)
	 * JOB_STAT_UNKWN  0x10000 (65536)
	 */
	jobStatus = job->status;

	return jobStatus;
}

int
lsf_wait(jobId)
	int jobId;
{
	int doneMask;
	int jobStatus;

	doneMask = JOB_STAT_DONE | JOB_STAT_EXIT;

	jobStatus = lsf_status(jobId);
	while ((jobStatus & doneMask) == 0)
	{
		sleep(POLLING_INTERVAL);
		jobStatus = lsf_status(jobId);
	}

	return 0;
}

int lsf_kill(jobId)
	int jobId;
{
	int status;

	status = lsb_signaljob(jobId, SIGKILL);

	if (status < 0)
	{
		lsb_perror("lsb_signaljob");
		return status;
	}

	return status;
}


/*
 * Python wrappers for the LSF interface functions.
 */
static PyObject *
PyLSF_submit(self, args, kwargs)
	PyObject *self;
	PyObject *args;
	PyObject *kwargs;
{
	int jobId;

    const char *command;
	const char *jobName = NULL;
    const char *queue = NULL;
    const char *stdout = NULL;
    const char *stderr = NULL;

    static char *kwlist[] = {"command", "jobName", "queue", "stdout", "stderr", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|zzzz", kwlist, &command, &jobName, &queue, &stdout, &stderr))
        return NULL;

    jobId = lsf_submit(command, jobName, queue, stdout, stderr);

    return Py_BuildValue("i", jobId);
}

static PyObject *
PyLSF_status(self, args)
	PyObject *self;
	PyObject *args;
{
	int status;

	int jobId;

    if (!PyArg_ParseTuple(args, "i", &jobId))
        return NULL;

    status = lsf_status(jobId);

    return Py_BuildValue("i", status);
}

static PyObject *
PyLSF_wait(self, args)
	PyObject *self;
	PyObject *args;
{
	int status;

	int jobId;

    if (!PyArg_ParseTuple(args, "i", &jobId))
        return NULL;

    status = lsf_wait(jobId);

    return Py_BuildValue("i", status);
}

static PyObject *
PyLSF_kill(self, args)
	PyObject *self;
	PyObject *args;
{
	int status;

	int jobId;

    if (!PyArg_ParseTuple(args, "i", &jobId))
        return NULL;

    status = lsf_kill(jobId);

    return Py_BuildValue("i", status);
}

/*
 * Doc strings for Python wrapper.
 */
PyDoc_STRVAR(submit__doc__,
	"submit(command, jobName=None, queue=None, stdout=None, stderr=None) -> int\n"
	"\n"
	"Submit an LSF job.\n"
	"\n"
	"Returns a jobId.\n"
);

PyDoc_STRVAR(status__doc__,
	"status(jobId)\n"
	"\n"
	"Get the status of an LSF job.\n"
);

PyDoc_STRVAR(wait__doc__,
	"wait(jobId)\n"
	"\n"
	"Wait for an LSF job to complete.\n"
);

PyDoc_STRVAR(kill__doc__,
	"kill(jobId)\n"
	"\n"
	"Kill an LSF job.\n"
);

/*
 * Initialize the Python module.
 *
 * Define the module's method table and initialization function.
 */
static PyMethodDef PyLSFMethods[] = {
	{
		"submit",
		(PyCFunction) PyLSF_submit,
		METH_VARARGS | METH_KEYWORDS,
		submit__doc__
	},
	{
		"status",
		PyLSF_status,
		METH_VARARGS,
		status__doc__
	},
	{
		"wait",
		PyLSF_wait,
		METH_VARARGS,
		wait__doc__
	},
	{
		"kill",
		PyLSF_kill,
		METH_VARARGS,
		kill__doc__
	},
	{NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC
initPyLSF(void)
{
	(void) Py_InitModule("PyLSF", PyLSFMethods);
}
