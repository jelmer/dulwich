#!/usr/bin/env python3
"""Example of using callback-based authentication with dulwich HTTP client.

This example demonstrates how to use the new callback-based authentication
feature to handle HTTP and proxy authentication dynamically.

Note: Dulwich currently supports 'basic' and 'anyauth' proxy authentication
methods via the http.proxyAuthMethod git config option or the
GIT_HTTP_PROXY_AUTHMETHOD environment variable. Other methods like 'digest',
'negotiate', and 'ntlm' will raise NotImplementedError.
"""

from dulwich.client import HttpGitClient


def my_auth_callback(url, www_authenticate, attempt):
    """Callback function for HTTP authentication.

    Args:
        url: The URL that requires authentication
        www_authenticate: The WWW-Authenticate header value from the server
        attempt: The attempt number (starts at 1)

    Returns:
        dict: Credentials with 'username' and 'password' keys, or None to cancel
    """
    print(f"Authentication required for {url}")
    print(f"Server says: {www_authenticate}")
    print(f"Attempt {attempt} of 3")

    # In a real application, you might:
    # - Prompt the user for credentials
    # - Look up credentials in a password manager
    # - Parse the www_authenticate header to determine the auth scheme

    if attempt <= 2:
        # Example: return hardcoded credentials for demo
        return {"username": "myuser", "password": "mypassword"}
    else:
        # Give up after 2 attempts
        return None


def my_proxy_auth_callback(url, proxy_authenticate, attempt):
    """Callback function for proxy authentication.

    Args:
        url: The URL being accessed through the proxy
        proxy_authenticate: The Proxy-Authenticate header value from the proxy
        attempt: The attempt number (starts at 1)

    Returns:
        dict: Credentials with 'username' and 'password' keys, or None to cancel
    """
    print(f"Proxy authentication required for accessing {url}")
    print(f"Proxy says: {proxy_authenticate}")

    # Return proxy credentials
    return {"username": "proxyuser", "password": "proxypass"}


def main():
    # Create an HTTP Git client with authentication callbacks
    client = HttpGitClient(
        "https://github.com/private/repo.git",
        auth_callback=my_auth_callback,
        proxy_auth_callback=my_proxy_auth_callback,
    )

    # Now when you use the client, it will call your callbacks
    # if authentication is required
    try:
        # Example: fetch refs from the repository
        refs = client.fetch_refs("https://github.com/private/repo.git")
        print(f"Successfully fetched refs: {refs}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
