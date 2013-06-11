#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <Python.h>
#include <lsf/lsbatch.h>
#include <lsf/lsf.h>

#define LSF_APP_NAME "PyLSF"
#define POLLING_INTERVAL 5 /* seconds */

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
#define IS_UNFINISHED(status) ((status & (JOB_STAT_DONE | JOB_STAT_EXIT)) == 0)

static void catch_sigint(int);

int lsf_submit(const char *, const char *, const char *, const char *, const char *, const char *);
int lsf_status(int);
void lsf_wait(int);
int lsf_kill(int);
int lsf_batch_status(char *);
void lsf_batch_wait(char *);
int lsf_batch_kill(char *);

static PyObject *PyLSF_submit(PyObject *, PyObject *, PyObject *);
static PyObject *PyLSF_status(PyObject *, PyObject *);
static PyObject *PyLSF_wait(PyObject *, PyObject *);
static PyObject *PyLSF_kill(PyObject *, PyObject *);
static PyObject *PyLSF_batch_status(PyObject *, PyObject *);
static PyObject *PyLSF_batch_wait(PyObject *, PyObject *);
static PyObject *PyLSF_batch_kill(PyObject *, PyObject *);

/*
 * Signal handling functions.
 */

static int terminated = 0;
static void
catch_sigint(int signal)
{
	terminated = 1;
}

/*
 * LSF interface functions.
 */

int
lsf_submit(command, jobName, queue, resReq, stdout, stderr)
	const char *command;
	const char *jobName;
	const char *queue;
	const char *resReq;
	const char *stdout;
	const char *stderr;
{
	struct submit req;
	struct submitReply reply;
	LS_LONG_INT jobId;
	int i;

	/*
	 * Initialize LSF connection.
	 */
	if (lsb_init(LSF_APP_NAME) < 0)
	{
		lsb_perror("lsb_init");
		return -1;
	}

	/*
	 * Prepare LSF submit request.
	 */
	memset(&req, 0, sizeof(struct submit));

	for (i = 0; i < LSF_RLIM_NLIMITS; i++)
	{
		req.rLimits[i] = DEFAULT_RLIMIT;
	}

	req.numProcessors = 1;
	req.maxNumProcessors = 1;

	req.command = (char *)command;

	// Make re-runnable by default.
	req.options |= SUB_RERUNNABLE;
	// This should be an option ...

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

	if (resReq != NULL)
	{
		req.resReq = (char *)resReq;
		req.options |= SUB_RES_REQ;
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
	struct jobInfoEnt *job;

	if (lsb_init(LSF_APP_NAME) < 0)
	{
		lsb_perror("lsb_init");
		return -1;
	}

	if (lsb_openjobinfo(jobId, NULL, NULL, NULL, NULL, ALL_JOB) < 0)
	{
		lsb_perror("lsb_openjobinfo");
		return -1;
	}

	job = lsb_readjobinfo(NULL);
	if (job == NULL)
	{
		lsb_perror("lsb_readjobinfo");
		return -1;
	}

	lsb_closejobinfo();

	return job->status;
}

void
lsf_wait(jobId)
	int jobId;
{
	int jobStatus;

	signal(SIGINT, catch_sigint);

	jobStatus = lsf_status(jobId);
	while (IS_UNFINISHED(jobStatus))
	{
		sleep(POLLING_INTERVAL);
		jobStatus = lsf_status(jobId);

		if (terminated)
		{
			terminated = 0;
			break;
		}
	}
}

int
lsf_kill(jobId)
	int jobId;
{
	if (lsb_init(LSF_APP_NAME) < 0)
	{
		lsb_perror("lsb_init");
		return -1;
	}

	if (lsb_signaljob(jobId, SIGKILL) < 0)
	{
		lsb_perror("lsb_signaljob");
		return -1;
	}

	return 0;
}

int
lsf_batch_status(jobName)
	char *jobName;
{
	struct jobInfoEnt *job;
	int more;
	int numJobsUnfinished;

	if (lsb_init(LSF_APP_NAME) < 0)
	{
		lsb_perror("lsb_init");
		return -1;
	}

	if (lsb_openjobinfo(0, jobName, NULL, NULL, NULL, ALL_JOB) < 0)
	{
		lsb_perror("lsb_openjobinfo");
		return -1;
	}

	more = 1;
	numJobsUnfinished = 0;

	while (more)
	{
		job = lsb_readjobinfo(&more);

		if (job == NULL)
		{
			lsb_perror("lsb_readjobinfo");
			return -1;
		}

		if (IS_UNFINISHED(job->status))
		{
			numJobsUnfinished++;
		}
	}

	lsb_closejobinfo();

	return numJobsUnfinished;
}

void
lsf_batch_wait(jobName)
	char *jobName;
{
	int numJobsRemaining;

	signal(SIGINT, catch_sigint);

	numJobsRemaining = lsf_batch_status(jobName);
	while (numJobsRemaining > 0)
	{
		sleep(POLLING_INTERVAL);
		numJobsRemaining = lsf_batch_status(jobName);

		if (terminated)
		{
			terminated = 0;
			break;
		}
	}
}

int
lsf_batch_kill(jobName)
	char *jobName;
{
	struct jobInfoEnt *job;
	int more;

	if (lsb_init(LSF_APP_NAME) < 0)
	{
		lsb_perror("lsb_init");
		return -1;
	}

	if (lsb_openjobinfo(0, jobName, NULL, NULL, NULL, ALL_JOB) < 0)
	{
		lsb_perror("lsb_openjobinfo");
		return -1;
	}

	more = 1;

	while (more)
	{
		job = lsb_readjobinfo(&more);

		if (job == NULL)
		{
			lsb_perror("lsb_readjobinfo");
			return -1;
		}

		if (IS_UNFINISHED(job->status))
		{
			if (lsb_signaljob(job->jobId, SIGKILL) < 0)
			{
				lsb_perror("lsb_signaljob");
				return -1;
			}
		}
	}

	lsb_closejobinfo();

	return 0;
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
    const char *resReq = NULL;
    const char *stdout = NULL;
    const char *stderr = NULL;

    static char *kwlist[] = {"command", "jobName", "queue", "resReq", "stdout", "stderr", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|zzzzz", kwlist, &command, &jobName, &queue, &resReq, &stdout, &stderr))
        return NULL;

    jobId = lsf_submit(command, jobName, queue, resReq, stdout, stderr);

    return Py_BuildValue("i", jobId);
}

static PyObject *
PyLSF_status(self, args)
	PyObject *self;
	PyObject *args;
{
	int status;

	const int jobId;

    if (!PyArg_ParseTuple(args, "i", &jobId))
        return NULL;

    status = lsf_status((int)jobId);

    return Py_BuildValue("i", status);
}

static PyObject *
PyLSF_wait(self, args)
	PyObject *self;
	PyObject *args;
{
	const int jobId;

    if (!PyArg_ParseTuple(args, "i", &jobId))
        return NULL;

    lsf_wait((int)jobId);

    return Py_None;
}

static PyObject *
PyLSF_kill(self, args)
	PyObject *self;
	PyObject *args;
{
	const int jobId;

    if (!PyArg_ParseTuple(args, "i", &jobId))
        return NULL;

    lsf_kill((int)jobId);

    return Py_None;
}

static PyObject *
PyLSF_batch_status(self, args)
	PyObject *self;
	PyObject *args;
{
	int numJobsRemaining;

	const char *jobName;

    if (!PyArg_ParseTuple(args, "s", &jobName))
        return NULL;

    numJobsRemaining = lsf_batch_status((char *)jobName);

    return Py_BuildValue("i", numJobsRemaining);
}

static PyObject *
PyLSF_batch_wait(self, args)
	PyObject *self;
	PyObject *args;
{
	const char *jobName;

    if (!PyArg_ParseTuple(args, "s", &jobName))
        return NULL;

    lsf_batch_wait((char *)jobName);

    return Py_None;
}

static PyObject *
PyLSF_batch_kill(self, args)
	PyObject *self;
	PyObject *args;
{
	const char *jobName;

    if (!PyArg_ParseTuple(args, "s", &jobName))
        return NULL;

    lsf_batch_kill((char *)jobName);

    return Py_None;
}

/*
 * Doc strings for Python wrapper.
 */
PyDoc_STRVAR(submit__doc__,
	"submit(command, jobName=None, queue=None, resReq=None, stdout=None, stderr=None) -> int\n"
	"\n"
	"Submit an LSF job.\n"
	"\n"
	"Returns a jobId.\n"
);

PyDoc_STRVAR(status__doc__,
	"status(jobName)\n"
	"\n"
	"Return the status of an LSF job.\n"
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

PyDoc_STRVAR(batch_status__doc__,
	"batch_status(jobName)\n"
	"\n"
	"Return the number of incomplete jobs in an LSF batch.\n"
);

PyDoc_STRVAR(batch_wait__doc__,
	"batch_wait(jobName)\n"
	"\n"
	"Wait for a batch of LSF jobs to complete.\n"
);

PyDoc_STRVAR(batch_kill__doc__,
	"batch_kill(jobName)\n"
	"\n"
	"Kill a batch of LSF jobs.\n"
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
	{
		"batch_status",
		PyLSF_batch_status,
		METH_VARARGS,
		batch_status__doc__
	},
	{
		"batch_wait",
		PyLSF_batch_wait,
		METH_VARARGS,
		batch_wait__doc__
	},
	{
		"batch_kill",
		PyLSF_batch_kill,
		METH_VARARGS,
		batch_kill__doc__
	},
	{NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC
initPyLSF(void)
{
	(void) Py_InitModule("PyLSF", PyLSFMethods);
}
