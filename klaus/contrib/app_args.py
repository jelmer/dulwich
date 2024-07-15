import os


def strtobool(val):
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return 1
    elif val in ("n", "no", "f", "false", "off", "0"):
        return 0
    else:
        raise ValueError(f"invalid truth value {val!r}")


def get_args_from_env():
    repos = os.environ.get("KLAUS_REPOS", [])
    if repos:
        repos = repos.split()
    args = (repos, os.environ.get("KLAUS_SITE_NAME", "unnamed site"))
    kwargs = dict(
        htdigest_file=os.environ.get("KLAUS_HTDIGEST_FILE"),
        use_smarthttp=strtobool(os.environ.get("KLAUS_USE_SMARTHTTP", "0")),
        require_browser_auth=strtobool(
            os.environ.get("KLAUS_REQUIRE_BROWSER_AUTH", "0")
        ),
        disable_push=strtobool(os.environ.get("KLAUS_DISABLE_PUSH", "0")),
        unauthenticated_push=strtobool(
            os.environ.get("KLAUS_UNAUTHENTICATED_PUSH", "0")
        ),
        ctags_policy=os.environ.get("KLAUS_CTAGS_POLICY", "none"),
    )
    return args, kwargs
