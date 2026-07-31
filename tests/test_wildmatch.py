# test_wildmatch.py -- Tests for bracket expressions following Git's wildmatch()
# Copyright (C) 2026 Vincent Gao <gaobing1230@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

"""Tests for :mod:`dulwich.wildmatch`."""

import re

from dulwich.wildmatch import MalformedPattern, translate_bracket_expression


def _matches(bracket: bytes, candidate: bytes) -> bool:
    """Whether ``bracket`` (a full ``[...]`` expression) matches one byte."""
    _, frag = translate_bracket_expression(bracket + b"Z", 1)
    return re.compile(rb"^" + frag + rb"$").match(candidate) is not None


def test_posix_classes():
    assert _matches(b"[[:digit:]]", b"5")
    assert not _matches(b"[[:digit:]]", b"a")
    assert _matches(b"[[:alpha:]]", b"a")
    assert not _matches(b"[[:alpha:]]", b"1")
    assert _matches(b"[[:space:]]", b" ")
    assert _matches(b"[[:upper:]]", b"A")
    assert not _matches(b"[[:upper:]]", b"a")


def test_negation():
    assert _matches(b"[^a-c]", b"d")
    assert not _matches(b"[^a-c]", b"b")
    assert _matches(b"[!a-c]", b"d")
    assert not _matches(b"[!a-c]", b"b")


def test_backslash_escapes_member():
    assert _matches(b"[a\\-c]", b"-")
    assert not _matches(b"[a\\-c]", b"b")


def test_class_never_matches_slash():
    assert not _matches(b"[^/]", b"/")


def test_inverted_range_keeps_low_end():
    # Git's wildmatch() takes "z" as a member before seeing the inverted "-a".
    assert _matches(b"[z-a]", b"z")
    assert not _matches(b"[z-a]", b"a")


def test_composed_class():
    assert _matches(b"[[:digit:]a-f_]", b"_")
    assert _matches(b"[[:digit:]a-f_]", b"c")
    assert not _matches(b"[[:digit:]a-f_]", b"g")


def test_malformed_patterns():
    for pat in (b"[abc", b"[[:bogus:]]", b"[[:^alpha:]]"):
        try:
            translate_bracket_expression(pat + b"Z", 1)
        except MalformedPattern:
            pass
        else:
            raise AssertionError(f"{pat!r} should be malformed")
