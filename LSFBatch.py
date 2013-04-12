import os
import sys
import shutil
import PyLSF

class LSFBatch:
    Status = (
        (0x00000, 'NULL'),
        (0x00001, 'PEND'),
        (0x00002, 'PSUSP'),
        (0x00004, 'RUN'),
        (0x00008, 'SSUSP'),
        (0x00010, 'USUSP'),
        (0x00020, 'EXIT'),
        (0x00040, 'DONE'),
        (0x00080, 'PDONE'),
        (0x00100, 'PERR'),
        (0x00200, 'WAIT'),
        (0x10000, 'UNKWN'),
    )

    def __init__(self, cmd, args, queue=None, memory=-1):
        """
        cmd   : a command string, with named template fields.
        args  : a dictionary mapping keys to lists of values.
        queue : LSF queue to use.
        
        To run 'someProgram' like this:
        
        $ someProgram -a file1 -b X -c 1
        $ someProgram -a file2 -b Y -c 2
        $ someProgram -a file3 -b Z -c 3
        
        Create a parameterized command string, and
        generate a dict of lists for each argument:
        
        >>> cmd  = 'someProgram -a %(argA)s -b %(argB)s -c %(argC)s
        >>> args = {'argA' : ['file1', 'file2', 'file3'],
                    'argB' : ['X', 'Y', 'Z'],
                    'argC' : [1.1, 2.2, 3.3]}
        >>> jobs = LSFBatch(cmd, args)
        >>> jobs.submit()
        """
        self._cmd = cmd
        self._args = args
        self._queue = queue
        self._memory = memory

        self._jobIds = []

        self._validate_args_shape(args)
        self._validate_args_content(cmd, args)
        self._commands = self._generate_commands(cmd, args)

    def _validate_args_shape(self, args):
        k0, v0 = args.iteritems().next()
        num_items = len(v0)

        # Validate shape of args dict.
        for k, v in args.iteritems():
            if len(v) != num_items:
                print 'Args dict contains value lists of different lengths:'
                print '* Key "%s" has %d items.' % (k0, len(v0))
                print '* Key "%s" has %d items.' % (k, len(v))
                sys.exit(1)

    def _validate_args_content(self, cmd, args):
        # Check that all required args have been provided.
        try:
            cmd % {k : v[0] for k, v in args.items()}
        except KeyError:
            print 'Args dict does not match command:'
            print '* cmd:  ', cmd
            print '* keys: ', args.keys()
            sys.exit(1)

    def _generate_commands(self, cmd, args):
        return [cmd % args_dict
                for args_dict in self._transform_args(args)]

    def _transform_args(self, args):
        """
        Transform a dict of lists into a list of dicts.
        
        Example:
            in:  {'a': [1, 2, 3],
                  'b': [4, 5, 6]}
            out: [{'a': 1, 'b': 4},
                  {'a': 2, 'b': 5},
                  {'a': 3, 'b': 6}]
        
        Thus, each dict from the output dict list can
        plug directly into the cmd string.
        """
        num_items = len(args.itervalues().next())

        return [{k : v[n] for k, v in args.iteritems()}
                for n in xrange(num_items)]

    def submit(self):
        for n, command in enumerate(self._commands):
            jobName = 'Lincer_%03d' % n
            queue = self._queue
            memory = self._memory
            stdout = 'stdout.' + jobName
            stderr = 'stderr.' + jobName

            jobId = PyLSF.submit(command, jobName, queue, memory, stdout, stderr)
            self._jobIds.append(jobId)

    def status(self, text=False):
        for jobId in self._jobIds:
            status = PyLSF.status(jobId)

            if text:
                flags = [stat
                         for flag, stat in self.Status
                         if (status & flag) != 0]
                flags = '|'.join(flags)
            else:
                flags = str(status)

            print 'Job <%d> : %s' % (jobId, flags)

    def wait(self):
        for jobId in self._jobIds:
            PyLSF.wait(jobId)

    def kill(self):
        for jobId in self._jobIds:
            PyLSF.kill(jobId)
