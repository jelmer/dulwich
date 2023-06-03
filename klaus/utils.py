# encoding: utf-8
import binascii
import os
import re
import time
import datetime
import mimetypes
import locale
import warnings
import subprocess

try:
    import chardet
except ImportError:
    chardet = None

from werkzeug.middleware.proxy_fix import ProxyFix as WerkzeugProxyFix
from humanize import naturaltime


class ProxyFix(WerkzeugProxyFix):
    """This middleware can be applied to add HTTP (reverse) proxy support to a
    WSGI application (klaus), making it possible to:

    * Mount it under a sub-URL (http://example.com/git/...)
    * Use a different HTTP scheme (HTTP vs. HTTPS)
    * Make it appear under a different domain altogether

    It sets `REMOTE_ADDR`, `HTTP_HOST` and `wsgi.url_scheme` from `X-Forwarded-*`
    headers.  It also sets `SCRIPT_NAME` from the `X-Script-Name` header.

    For instance if you have klaus mounted under /git/ and your site uses SSL
    (but your proxy doesn't), make the proxy pass ::

        X-Script-Name = '/git'
        X-Forwarded-Proto = 'https'
        ...

    If you have more than one proxy server in front of your app, set
    `num_proxies` accordingly.

    Do not use this middleware in non-proxy setups for security reasons.

    The original values of `REMOTE_ADDR` and `HTTP_HOST` are stored in
    the WSGI environment as `werkzeug.proxy_fix.orig_remote_addr` and
    `werkzeug.proxy_fix.orig_http_host`.

    :param app: the WSGI application
    :param num_proxies: the number of proxy servers in front of the app.
    """

    def __call__(self, environ, start_response):
        script_name = environ.get("HTTP_X_SCRIPT_NAME")
        if script_name is not None:
            if script_name.endswith("/"):
                warnings.warn(
                    "'X-Script-Name' header should not end in '/' (found: %r). "
                    "Please fix your proxy's configuration." % script_name
                )
                script_name = script_name.rstrip("/")
            environ["SCRIPT_NAME"] = script_name
        return super(ProxyFix, self).__call__(environ, start_response)


class SubUri(object):
    """WSGI middleware to tweak the WSGI environ so that it's possible to serve
    the wrapped app (klaus) under a sub-URL and/or to use a different HTTP
    scheme (http:// vs. https://) for proxy communication.

    This is done by making your proxy pass appropriate HTTP_X_SCRIPT_NAME and
    HTTP_X_SCHEME headers.

    For instance if you have klaus mounted under /git/ and your site uses SSL
    (but your proxy doesn't), make it pass ::

        X-Script-Name = '/git'
        X-Scheme = 'https'

    Snippet stolen from http://flask.pocoo.org/snippets/35/
    """

    def __init__(self, app):
        warnings.warn(
            "'klaus.utils.SubUri' is deprecated and will be removed. "
            "Please upgrade your code to use 'klaus.utils.ProxyFix' instead.",
            DeprecationWarning,
        )
        self.app = app

    def __call__(self, environ, start_response):
        script_name = environ.get("HTTP_X_SCRIPT_NAME", "")
        if script_name:
            environ["SCRIPT_NAME"] = script_name.rstrip("/")

        if script_name and environ["PATH_INFO"].startswith(script_name):
            # strip `script_name` from PATH_INFO
            environ["PATH_INFO"] = environ["PATH_INFO"][len(script_name) :]

        if "HTTP_X_SCHEME" in environ:
            environ["wsgi.url_scheme"] = environ["HTTP_X_SCHEME"]

        return self.app(environ, start_response)


def timesince(when, now=time.time):
    """Return the difference between `when` and `now` in human readable form."""
    return naturaltime(now() - when)


def formattimestamp(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).strftime("%b %d, %Y %H:%M:%S")


def guess_is_binary(dulwich_blob):
    return any(b"\0" in chunk for chunk in dulwich_blob.chunked)


def guess_is_image(filename):
    mime, _ = mimetypes.guess_type(filename)
    if mime is None:
        return False
    return mime.startswith("image/")


def encode_for_git(s):
    # XXX This assumes everything to be UTF-8 encoded
    return s.encode("utf8")


def decode_from_git(b):
    # XXX This assumes everything to be UTF-8 encoded
    return b.decode("utf8")


def force_unicode(s):
    """Do all kinds of magic to turn `s` into unicode"""
    # It's already unicode, don't do anything:
    try:
        # Python < 3
        text_type = unicode
    except NameError:
        text_type = str
    if isinstance(s, text_type):
        return s

    # Try some default encodings:
    try:
        return s.decode("utf-8")
    except UnicodeDecodeError:
        pass

    try:
        return s.decode(locale.getpreferredencoding())
    except UnicodeDecodeError:
        pass

    if chardet is not None:
        # Try chardet, if available
        encoding = chardet.detect(s)["encoding"]
        if encoding is not None:
            try:
                return s.decode(encoding, 'replace')
            except LookupError:
                pass

    return s.decode('latin1', 'replace')


def extract_author_name(email):
    """Extract the name from an email address --
    >>> extract_author_name("John <john@example.com>")
    "John"

    -- or return the address if none is given.
    >>> extract_author_name("noname@example.com")
    "noname@example.com"
    """
    match = re.match("^(.*?)<.*?>$", email)
    if match:
        return match.group(1).strip()
    return email


def is_hex_prefix(s):
    if len(s) % 2:
        s += "0"
    try:
        binascii.unhexlify(s)
        return True
    except binascii.Error:
        return False


def shorten_sha1(sha1):
    if 20 <= len(sha1) <= 40 and is_hex_prefix(sha1):
        sha1 = sha1[:7]
    return sha1


def parent_directory(path):
    return os.path.split(path)[0]


def subpaths(path):
    """Yield a `(last part, subpath)` tuple for all possible sub-paths of `path`.

    >>> list(subpaths("foo/bar/spam"))
    [('foo', 'foo'), ('bar', 'foo/bar'), ('spam', 'foo/bar/spam')]
    """
    seen = []
    for part in path.split("/"):
        seen.append(part)
        yield part, "/".join(seen)


def shorten_message(msg):
    return msg.split("\n")[0]


def replace_dupes(ls, replacement):
    """Replace items in `ls` that are equal to their predecessors with `replacement`.

    >>> ls = [1, 2, 2, 3, 2, 2, 2]
    >>> replace_dupes(x, 'x')
    >>> ls
    [1, 2, 'x', 3, 2, 'x', 'x']
    """
    last = object()
    for i, elem in enumerate(ls):
        if last == elem:
            ls[i] = replacement
        else:
            last = elem


def guess_git_revision():
    """Try to guess whether this instance of klaus is run directly from a klaus
    git checkout.  If it is, guess and return the currently checked-out commit
    SHA.  If it's not (installed using pip, setup.py or the like), return None.

    This is used to display the "powered by klaus $VERSION" footer on each page,
    $VERSION being either the SHA guessed by this function or the latest release number.
    """
    git_dir = os.path.join(os.path.dirname(__file__), "..", ".git")
    try:
        return force_unicode(
            subprocess.check_output(
                ["git", "log", "--format=%h", "-n", "1"], cwd=git_dir
            ).strip()
        )
    except OSError:
        # Either the git executable couldn't be found in the OS's PATH
        # or no ".git" directory exists, i.e. this is no "bleeding-edge" installation.
        return None


def sanitize_branch_name(name, chars="./", repl="-"):
    for char in chars:
        name = name.replace(char, repl)
    return name


def escape_html(s):
    return (
        s.replace(b"&", b"&amp;")
        .replace(b"<", b"&lt;")
        .replace(b">", b"&gt;")
        .replace(b'"', b"&quot;")
    )


def tarball_basename(repo_name, rev):
    """Determine the name for a tarball."""
    rev = sanitize_branch_name(rev, chars="/")
    if rev.startswith(repo_name + "-"):
        # If the rev is a tag name that already starts with the repo name,
        # skip it.
        return rev
    elif len(rev) >= 2 and rev[0] == "v" and not rev[1].isalpha():
        # If the rev is a tag name prefixed by a 'v', skip the 'v'.
        # So, v-1.0 -> 1.0, v1.0 -> 1.0, but vanilla -> vanilla.
        return "%s-%s" % (repo_name, rev[1:])
    elif len(rev) == 40 and is_hex_prefix(rev):
        # If the rev is a commit hash, simply use that.
        return "%s@%s" % (repo_name, rev)
    else:
        return "%s-%s" % (repo_name, rev)


def repo_human_name(path):
    """Get repository name from path.

    1. /x/y.git -> /x/y  and  /x/y/.git/ -> /x/y//
    2. /x/y/ -> /x/y
    3. /x/y -> y
    """
    name = path.rstrip(os.sep).split(os.sep)[-1]
    if name.endswith(".git"):
        name = name[:-4]
    return name
