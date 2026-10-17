"""Full recording tracebacks with credential and stream-URL redaction.

Format in an executor: Python's traceback formatter may read source files.
Frame filenames, functions, line numbers and exception types are retained.
Locals are never captured. Message paths, URLs and credential assignments are
redacted; source file paths in traceback frame headers remain visible.
"""

from __future__ import annotations

import re
import traceback

_URL = re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://[^\s<>\"']+")
_CREDENTIAL = re.compile(
    r"""(?ix)
    (?P<key>["']?\b[a-z0-9_-]*
    (?:password|passwd|pwd|token|api[_-]?key|secret|authorization|cookie)
    [a-z0-9_-]*["']?\s*[:=]\s*)
    (?:"[^"\r\n]*"|'[^'\r\n]*'|[^\s,;}\]]+)
    """
)
_AUTH = re.compile(r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9+/=._~-]+")
_JWT = re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b")
_PATH = re.compile(r"""(?<![\w:])(?:[A-Za-z]:[\\/]|/)[^\s"'<>]+""")
_FRAME_HEADER = re.compile(r'^\s*(?:\|\s*)?File "')


def format_redacted_exception_traceback(exc: BaseException) -> str:
    """Render every normal traceback frame and chained cause without locals.

    Unknown exception types and modules are retained rather than replaced with
    generic labels. Redaction is applied to the complete rendered text, covering
    source lines, chained exceptions and exception notes as well as messages.
    Unlabelled secrets cannot in general be identified from arbitrary text.
    """
    try:
        captured = traceback.TracebackException.from_exception(
            exc, limit=None, capture_locals=False, lookup_lines=True
        )
        rendered = "".join(captured.format(chain=True))
        lines = []
        for line in rendered.splitlines(keepends=True):
            keep_source_path = _FRAME_HEADER.match(line) is not None
            line = _URL.sub("<redacted_url>", line)
            line = _AUTH.sub(lambda match: match[1] + " <redacted>", line)
            line = _CREDENTIAL.sub(lambda match: match["key"] + "<redacted>", line)
            line = _JWT.sub("<redacted_token>", line)
            if not keep_source_path:
                line = _PATH.sub("<redacted_path>", line)
            lines.append(line)
        return "".join(lines)
    except Exception:
        return "Traceback unavailable (diagnostic formatting failed)."
