'''Utility funtions and classes shared between edmCfgSplitter and kuntur scripts
'''

import argparse
import sys
import os.path as path


# -----------------------------------------------------------------------------
class JidInserter(object):
    """Generic utility class to add job id to filenames"""

    def __init__(self, njobs):
        super(JidInserter, self).__init__()
        self._njobs = njobs
        self._fmt = '{{}}_{{:0{}d}}{{}}'.format(len(str(njobs)))

    def __call__(self, fileName, i):
        fileString = fileName if isinstance(fileName, str) else fileName.value()
        name, ext = path.splitext(fileString)
        return fileName.__class__(self._fmt.format(name, i, ext))
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
class CheckExt(argparse.Action):
    '''Utility class to enforce specific extensions on argparse arguments
    '''

    def __init__(self, *args, **kwargs):
        # if 'choiches' in kwargs:
        self._choices = kwargs.pop('choiches')
        super(CheckExt, self).__init__(*args, **kwargs)
        # self._choices = choiches

    def __call__(self, parser, namespace, fname, option_string=None):
        ext = path.splitext(fname)[1][1:]
        if ext not in self._choices:
            option_string = '({})'.format(option_string) if option_string else ''
            parser.error("file doesn't end with one of {}{}".format(self._choices, option_string))
        else:
            setattr(namespace, self.dest, fname)
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
def SplitList(lst, n):
    cls = lst.__class__
    # Group size (approximate)
    gs = len(lst) / n

    # Reminder
    rmndr = len(lst) % n

    grps = []
    for i in xrange(n):

        # Calculate the indexes
        if i < rmndr:
            # Groups with index lower than reminder get an additional element
            j, s = (i*(gs+1), gs+1)
        else:
            # Groups beyond have standard size
            j, s = (rmndr*(gs+1)+(i-rmndr)*gs, gs)

        # New
        grp = cls(lst[j:j+s])
        # print i, j, len(grp)
        grps += [grp]

    return grps
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
class SysArgVSentry(object):
    """RAII pattern to temporaryly modify command arguments
    """
    def __init__(self, argv):
        super(SysArgVSentry, self).__init__()
        self.argv = argv

    def __enter__(self):
        self._oldargv = sys.argv[:]
        sys.argv = self.argv
        return self

    def __exit__(self, type, value, traceback):
        sys.argv = self._oldargv
# -----------------------------------------------------------------------------
