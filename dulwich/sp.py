# Can't call this module "subprocess" because the next line would otherwise
# import ourselves unless using "from __future__ import absolute_import"
# which is not available before Python 2.5
from subprocess import *
# FIXME: should get every symbol from "subprocess" into our module
from subprocess import mswindows

import os

if os.name == 'nt':
    # subprocess.Popen on Windows only appends .exe if no extension is given
    # instead of following cmd semantics of searching PATH by PATHEXT
    #
    # Wrap Popen and resolve the correct extension and path before calling
    # the real Popen.
    #
    # See: http://bugs.python.org/issue8557
    #
    # Note: Popen still cannot execute e.g. Python or JScript files even if you
    # have '.py' or '.js' in PATHEXT
    #
    # PATH documentation: http://www.microsoft.com/resources/documentation/
    # windows/xp/all/proddocs/en-us/path.mspx

    def _resolve_ext(exe, environ):
        """If "exe" doesn't have an extension, find one by traversing
        PATH and PATHEXT and return a matching file with full path and
        extension. Otherwise return "exe"."""

        if os.path.splitext(exe)[1]:
            return exe

        dirname = os.path.dirname(exe)

        if dirname:
            path = [dirname]
        else:
            path = ['.'] + environ.get('PATH', '.').split(';')

        # Note: the above doc mixes up the order of .EXE and .COM
        pathext = environ.get('PATHEXT', '.COM;.EXE;.BAT;.CMD').split(';')

        for dir in path:
            for ext in pathext:
                fullpath = os.path.join(dir, exe + ext)
                if os.path.exists(fullpath):
                    return fullpath

        return exe

    _Popen = Popen
    class Popen(_Popen):
        def __init__(
            self, args, bufsize=0, executable=None, stdin=None, stdout=None,
            stderr=None, preexec_fn=None, close_fds=False, shell=False, 
            cwd=None, env=None, *args_, **kw
        ):
            if executable is None and not shell:
                if isinstance(args, basestring):
                    exe = args
                else:
                    exe = args[0]
                executable = _resolve_ext(exe, env or os.environ)

            super(Popen, self).__init__(
                args, bufsize, executable, stdin, stdout, stderr, 
                preexec_fn, close_fds, shell, cwd, env, *args_, **kw
            )
