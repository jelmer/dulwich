"""
Alternate to `Versioneer <https://pypi.python.org/pypi/versioneer/>`_ using
`Dulwich <https://pypi.python.org/pypi/dulwich>`_ to sort tags by time from
newest to oldest.

Import this module into the package ``__init__.py`` and then set ``__version__``
as follows::

    from dulwich.contrib.robotic_releases import get_recent_tags

    __version__ = get_recent_tags()[0][0][1:]
    # other dunder classes like __author__, etc.

This example assumes the tags have a leading "v" like "v0.3", and that the
``.git`` folder is in the project folder that containts the package folder.
"""

from dulwich.repo import Repo
import time
import datetime
import os

# CONSTANTS
DIRNAME = os.path.abspath(os.path.dirname(__file__))
PROJDIR = os.path.dirname(DIRNAME)

def get_recent_tags(projdir=PROJDIR):
    """
    Get list of recent tags in order from newest to oldest and their datetimes.

    :param projdir: path to ``.git``
    :returns: list of (tag, [datetime, commit, author]) sorted from new to old
    """
    project = Repo(projdir)  # dulwich repository object
    refs = project.get_refs()  # dictionary of refs and their SHA-1 values
    tags = {}  # empty dictionary to hold tags, commits and datetimes
    # iterate over refs in repository
    for key, value in refs.iteritems():
        obj = project.get_object(value)  # dulwich object from SHA-1
        # check if object is tag
        if obj.type_name != 'tag':
            # skip ref if not a tag
            continue
        # strip the leading text from "refs/tag/<tag name>" to get "tag name"
        _, tag = key.rsplit('/', 1)
        # check if tag object is commit, altho it should always be true
        if obj.object[0].type_name == 'commit':
            commit = project.get_object(obj.object[1])  # commit object
            # get tag commit datetime, but dulwich returns seconds since
            # beginning of epoch, so use Python time module to convert it to
            # timetuple then convert to datetime
            tags[tag] = [
                datetime.datetime(*time.gmtime(commit.commit_time)[:6]),
                commit.id,
                commit.author
            ]
            
    # return list of tags sorted by their datetimes from newest to oldest
    return sorted(tags.iteritems(), key=lambda tag: tag[1][0], reverse=True)
