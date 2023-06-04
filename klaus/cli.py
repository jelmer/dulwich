import argparse
import os
import sys
import webbrowser

from dulwich.errors import NotGitRepository
from dulwich.repo import Repo

from klaus import KLAUS_VERSION, make_app
from klaus.utils import force_unicode


def git_repository(path):
    path = os.path.abspath(path)
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError("%r: No such directory" % path)
    try:
        Repo(path)
    except NotGitRepository:
        raise argparse.ArgumentTypeError("%r: Not a Git repository" % path)
    return path


def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", help="default: 127.0.0.1", default="127.0.0.1")
    parser.add_argument("--port", help="default: 8080", default=8080, type=int)
    parser.add_argument(
        "--site-name", help="site name showed in header. default: your hostname"
    )
    parser.add_argument("--version", help="print version number", action="store_true")
    parser.add_argument(
        "-b",
        "--browser",
        help="open klaus in a browser on server start",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-B",
        "--with-browser",
        help="specify which browser to use with --browser",
        metavar="BROWSER",
        default=None,
    )
    parser.add_argument(
        "--ctags",
        help="enable ctags for which revisions? default: none. "
        "WARNING: Don't use 'ALL' for public servers!",
        choices=["none", "tags-and-branches", "ALL"],
        default="none",
    )

    parser.add_argument(
        "repos",
        help="repositories to serve",
        metavar="DIR",
        nargs="*",
        type=git_repository,
    )

    grp = parser.add_argument_group("Git Smart HTTP")
    grp.add_argument(
        "--smarthttp", help="enable Git Smart HTTP serving", action="store_true"
    )
    grp.add_argument(
        "--htdigest",
        help="use credentials from FILE",
        metavar="FILE",
        type=argparse.FileType("r"),
    )

    grp = parser.add_argument_group("Development flags", "DO NOT USE IN PRODUCTION!")
    grp.add_argument(
        "--debug", help="Enable Werkzeug debugger and reloader", action="store_true"
    )

    return parser


def main():
    args = make_parser().parse_args()

    if args.version:
        print(KLAUS_VERSION)
        return 0

    if args.htdigest and not args.smarthttp:
        print(
            "ERROR: --htdigest option has no effect without --smarthttp enabled",
            file=sys.stderr,
        )
        return 1

    if not args.repos:
        print(
            "WARNING: No repositories supplied -- syntax is 'klaus dir1 dir2...'.",
            file=sys.stderr,
        )

    if not args.site_name:
        args.site_name = "%s:%d" % (args.host, args.port)

    if args.ctags != "none":
        from klaus.ctagsutils import check_have_exuberant_ctags

        if not check_have_exuberant_ctags():
            print(
                "ERROR: Exuberant ctags not installed (or 'ctags' binary isn't *Exuberant* ctags)",
                file=sys.stderr,
            )
            return 1
        try:
            pass
        except ImportError:
            raise ImportError("Please install 'python-ctags3' to enable ctags support.")

    app = make_app(
        args.repos,
        force_unicode(args.site_name or args.host),
        args.smarthttp,
        args.htdigest,
        ctags_policy=args.ctags,
    )

    if args.browser:
        _open_browser(args)

    app.run(args.host, args.port, args.debug)


def _open_browser(args):
    # Open a web browser onto the server URL. Technically we're jumping the
    # gun a little here since the server is not yet running, but there's no
    # clean way to run a function after the server has started without
    # losing the simplicity of the code. In the Real World (TM) it'll take
    # longer for the browser to start than it will for us to start
    # serving, so we'll be OK.
    if args.with_browser is None:
        opener = webbrowser.open
    else:
        opener = webbrowser.get(args.with_browser).open
    opener(f"http://{args.host}:{args.port}")


if __name__ == "__main__":
    exit(main())
