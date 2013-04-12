import sys
from time import strftime
import PyLSF

class LSFBatch:
    def __init__(self, cmd, args, jobName=None, queue=None, memory=-1):
        """
        cmd     : a command string, with named template fields.
        args    : a dictionary mapping keys to lists of values.
        jobName : a name for the job group.
        queue   : LSF queue to use.
        memory  : LSF memory to use.
        
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
        self._jobName = jobName
        self._queue = queue
        self._memory = memory

        if jobName is None:
            self._jobName = 'PyLSF_' + strftime('%H%m%S')

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
            kwargs = {
                'jobName' : self._jobName,
                'queue'   : self._queue,
                'memory'  : self._memory,
                'stdout'  : 'stdout.job_%d' % n,
                'stderr'  : 'stderr.job_%d' % n,
            }

            PyLSF.submit(command, **kwargs)

    def wait(self):
        PyLSF.batch_wait(self._jobName)

    def kill(self):
        PyLSF.batch_kill(self._jobName)
