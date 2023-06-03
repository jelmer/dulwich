import jinja2
try:
    import jinja2.ext.autoescape
    jinja2_autoescape_builtin = False
except ImportError:
    jinja2_autoescape_builtin = True
import flask
import httpauth
import dulwich.web
from dulwich.errors import NotGitRepository
from klaus import views, utils
from klaus.repo import FancyRepo, InvalidRepo


KLAUS_VERSION = utils.guess_git_revision() or "2.0.3"


class Klaus(flask.Flask):
    jinja_options = {
        "extensions": [] if jinja2_autoescape_builtin else ["jinja2.ext.autoescape"],
        "undefined": jinja2.StrictUndefined,
    }

    def __init__(self, repo_paths, site_name, use_smarthttp, ctags_policy="none"):
        """(See `make_app` for parameter descriptions.)"""
        self.site_name = site_name
        self.use_smarthttp = use_smarthttp
        self.ctags_policy = ctags_policy

        valid_repos, invalid_repos = self.load_repos(repo_paths)
        self.valid_repos = {repo.namespaced_name: repo for repo in valid_repos}
        self.invalid_repos = {repo.namespaced_name: repo for repo in invalid_repos}

        flask.Flask.__init__(self, __name__)

        self.setup_routes()

    def create_jinja_environment(self):
        """Called by Flask.__init__"""
        env = super(Klaus, self).create_jinja_environment()
        for func in [
            "force_unicode",
            "timesince",
            "shorten_sha1",
            "shorten_message",
            "extract_author_name",
            "formattimestamp",
        ]:
            env.filters[func] = getattr(utils, func)

        env.globals["KLAUS_VERSION"] = KLAUS_VERSION
        env.globals["USE_SMARTHTTP"] = self.use_smarthttp
        env.globals["SITE_NAME"] = self.site_name

        return env

    def setup_routes(self):
        # fmt: off
        for endpoint, rule in [
            ('repo_list',   '/'),
            ('robots_txt',  '/robots.txt/'),
            ('blob',        '/<repo>/blob/'),
            ('blob',        '/<repo>/blob/<rev>/<path:path>'),
            ('blame',       '/<repo>/blame/'),
            ('blame',       '/<repo>/blame/<rev>/<path:path>'),
            ('raw',         '/<repo>/raw/<path:path>/'),
            ('raw',         '/<repo>/raw/<rev>/<path:path>'),
            ('submodule',   '/<repo>/submodule/<rev>/'),
            ('submodule',   '/<repo>/submodule/<rev>/<path:path>'),
            ('commit',      '/<repo>/commit/<path:rev>/'),
            ('patch',       '/<repo>/commit/<path:rev>.diff'),
            ('patch',       '/<repo>/commit/<path:rev>.patch'),
            ('index',       '/<repo>/'),
            ('index',       '/<repo>/<path:rev>'),
            ('history',     '/<repo>/tree/<rev>/'),
            ('history',     '/<repo>/tree/<rev>/<path:path>'),
            ('download',    '/<repo>/tarball/<path:rev>/'),
        ]:
            self.add_url_rule(rule, view_func=getattr(views, endpoint))
            if "<repo>" in rule:
                self.add_url_rule(
                    "/~<namespace>" + rule, view_func=getattr(views, endpoint)
                )
        # fmt: on

    def should_use_ctags(self, git_repo, git_commit):
        if self.ctags_policy == "none":
            return False
        elif self.ctags_policy == "ALL":
            return True
        elif self.ctags_policy == "tags-and-branches":
            return git_commit.id in git_repo.get_tag_and_branch_shas()
        else:
            raise ValueError("Unknown ctags policy %r" % self.ctags_policy)

    def load_repos(self, repo_paths):
        valid_repos = []
        invalid_repos = []
        for namespace, paths in repo_paths.items():
            for path in paths:
                try:
                    valid_repos.append(FancyRepo(path, namespace))
                except NotGitRepository:
                    invalid_repos.append(InvalidRepo(path, namespace))
        return valid_repos, invalid_repos


def make_app(
    repo_paths,
    site_name,
    use_smarthttp=False,
    htdigest_file=None,
    require_browser_auth=False,
    disable_push=False,
    unauthenticated_push=False,
    ctags_policy="none",
):
    """
    Returns a WSGI app with all the features (smarthttp, authentication)
    already patched in.
    :param repo_paths: Repositories to serve. This can either be a list of paths
        or dictionary of the following form:
            {
                "namespace1": [list of paths of repositories],
                "namespace2": [list of paths of repositories],
                ...
                None: [list of paths of repositories without namespace]
            }
    :param site_name: Name of the Web site (e.g. "John Doe's Git Repositories")
    :param use_smarthttp: Enable Git Smart HTTP mode, which makes it possible to
        pull from the served repositories. If `htdigest_file` is set as well,
        also allow to push for authenticated users.
    :param require_browser_auth: Require HTTP authentication according to the
        credentials in `htdigest_file` for ALL access to the Web interface.
        Requires the `htdigest_file` option to be set.
    :param disable_push: Disable push support. This is required in case both
        `use_smarthttp` and `require_browser_auth` (and thus `htdigest_file`)
        are set, but push should not be supported.
    :param htdigest_file: A *file-like* object that contains the HTTP auth credentials.
    :param unauthenticated_push: Allow push'ing without authentication. DANGER ZONE!
    :param ctags_policy: The ctags policy to use, may be one of:
        - 'none': never use ctags
        - 'tags-and-branches': use ctags for revisions that are the HEAD of
          a tag or branc
        - 'ALL': use ctags for all revisions, may result in high server load!
    """
    if unauthenticated_push:
        if not use_smarthttp:
            raise ValueError("'unauthenticated_push' set without 'use_smarthttp'")
        if disable_push:
            raise ValueError("'unauthenticated_push' set with 'disable_push'")
        if require_browser_auth:
            raise ValueError(
                "Incompatible options 'unauthenticated_push' and 'require_browser_auth'"
            )
    if htdigest_file and not (require_browser_auth or use_smarthttp):
        raise ValueError(
            "'htdigest_file' set without 'use_smarthttp' or 'require_browser_auth'"
        )
    if not isinstance(repo_paths, dict):
        # If repos is given as a flat list, put all repos under the "no namespace" namespace
        repo_paths = {None: repo_paths}

    app = Klaus(
        repo_paths,
        site_name,
        use_smarthttp,
        ctags_policy,
    )
    app.wsgi_app = utils.ProxyFix(app.wsgi_app)

    if use_smarthttp:
        # `path -> Repo` mapping for Dulwich's web support
        dulwich_backend = dulwich.server.DictBackend(
            {
                "/" + namespaced_name: repo
                for namespaced_name, repo in app.valid_repos.items()
            }
        )
        # Dulwich takes care of all Git related requests/URLs
        # and passes through everything else to klaus
        dulwich_wrapped_app = dulwich.web.make_wsgi_chain(
            backend=dulwich_backend,
            fallback_app=app.wsgi_app,
        )
        dulwich_wrapped_app = utils.ProxyFix(dulwich_wrapped_app)

        # `receive-pack` is requested by the "client" on a push
        # (the "server" is asked to *receive* packs), i.e. we need to secure
        # it using authentication or deny access completely to make the repo
        # read-only.
        #
        # Git first sends requests to /<repo-name>/info/refs?service=git-receive-pack.
        # If this request is responded to using HTTP 401 Unauthorized, the user
        # is prompted for username and password. If we keep responding 401, Git
        # interprets this as an authentication failure.  (We can't respond 403
        # because this results in horrible, unhelpful Git error messages.)
        #
        # Git will never call /<repo-name>/git-receive-pack if authentication
        # failed for /info/refs, but since it's used to upload stuff to the server
        # we must secure it anyway for security reasons.
        PATTERN = (
            r"^/(~[^/]+/)?[^/]+/(info/refs\?service=git-receive-pack|git-receive-pack)$"
        )
        if unauthenticated_push:
            # DANGER ZONE: Don't require authentication for push'ing
            app.wsgi_app = dulwich_wrapped_app
        elif htdigest_file and not disable_push:
            # .htdigest file given. Use it to read the push-er credentials from.
            if require_browser_auth:
                # No need to secure push'ing if we already require HTTP auth
                # for all of the Web interface.
                app.wsgi_app = dulwich_wrapped_app
            else:
                # Web interface isn't already secured. Require authentication for push'ing.
                app.wsgi_app = httpauth.DigestFileHttpAuthMiddleware(
                    htdigest_file,
                    wsgi_app=dulwich_wrapped_app,
                    routes=[PATTERN],
                )
        else:
            # No .htdigest file given. Disable push-ing.  Semantically we should
            # use HTTP 403 here but since that results in freaky error messages
            # (see above) we keep asking for authentication (401) instead.
            # Git will print a nice error message after a few tries.
            app.wsgi_app = httpauth.AlwaysFailingAuthMiddleware(
                wsgi_app=dulwich_wrapped_app,
                routes=[PATTERN],
            )

    if require_browser_auth:
        app.wsgi_app = httpauth.DigestFileHttpAuthMiddleware(
            htdigest_file, wsgi_app=app.wsgi_app
        )

    return app
