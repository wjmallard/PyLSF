import time
import logging

import PyLSF

POLLING_INTERVAL = 5  # seconds

class QueueManager:

    def __init__(self):
        self._cmdList = []

    def add(self, cmd, stdout=None, stderr=None):
        self._cmdList.append((cmd, stdout, stderr))

    def run(self, maxJobs, jobName, queue=None, memory= -1):

        if len(self._cmdList) == 0:
            logging.warn('QueueManager.run() called with empty job queue.')
            return

        if maxJobs <= 0:
            logging.warn('Invalid value for maxJobs: %d' % maxJobs)
            return

        numJobsTotal = len(self._cmdList)
        logging.info('QueueManager starting %d jobs.' % numJobsTotal)

        firstIteration = True
        numJobsUnqueued = len(self._cmdList)

        while numJobsUnqueued > 0:

            if firstIteration:
                firstIteration = False
                numJobsQueued = 0
                numOpenSlots = min(maxJobs - numJobsQueued, numJobsUnqueued)
                numJobsFinished = 0
            else:
                numJobsQueued = PyLSF.batch_status(jobName)
                numOpenSlots = min(maxJobs - numJobsQueued, numJobsUnqueued)
                numJobsFinished += numOpenSlots

            logging.info('Submitting %d jobs. Completed %d of %d.'
                         % (numOpenSlots, numJobsFinished, numJobsTotal))

            for _ in xrange(numOpenSlots):
                cmd, stdout, stderr = self._cmdList.pop(0)
                PyLSF.submit(cmd,
                             jobName=jobName,
                             queue=queue,
                             memory=memory,
                             stdout=stdout,
                             stderr=stderr)

            time.sleep(POLLING_INTERVAL)
            numJobsUnqueued = len(self._cmdList)

        logging.info('All jobs submitted! Waiting for them to complete.')

        numJobsQueued = PyLSF.batch_status(jobName)
        while numJobsQueued > 0:
            time.sleep(POLLING_INTERVAL)
            numJobsQueued = PyLSF.batch_status(jobName)

        logging.info('All jobs completed.')
