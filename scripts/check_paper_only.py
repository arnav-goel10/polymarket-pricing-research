#!/usr/bin/env python3
"""Fail CI if a public external-order or credential surface is introduced."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]

PUBLIC_ROOTS = (
    "crates",
    "scripts",
    ".github",
    "docs",
    "demo",
    "fixtures",
    "Cargo.toml",
    "Cargo.lock",
    "Dockerfile",
    ".env.example",
    "README.md",
    "SECURITY.md",
)

IGNORED_DIRECTORIES = {".git", "target", "node_modules", "__pycache__"}


@dataclass(frozen=True)
class Finding:
    """A forbidden capability located in a public repository file."""

    path: str
    line: int
    reason: str


# These expressions intentionally encode capability families instead of a narrow
# list of exact spellings. Their regex notation also prevents this scanner from
# flagging its own definitions when ``scripts`` is scanned.
FORBIDDEN = {
    "credential or signer capability": re.compile(
        r"(?:private|secret)[_-]?key|local(?:wallet|signer)|"
        r"deposit[_-]?wallet[_-]?address",
        re.IGNORECASE,
    ),
    "external order-submission capability": re.compile(
        r"(?:submit|create|post|cancel)[_-]?order|"
        r"build[_-]?sign[_-]?and[_-]?post",
        re.IGNORECASE,
    ),
    "exchange SDK dependency": re.compile(
        r"polymarket[_-]?client[_-]?sdk",
        re.IGNORECASE,
    ),
}

HTTP_WRITE_METHOD = re.compile(
    r"(?:(?:-X|--request(?:=|\s+))\s*"
    r"(?P<flag_method>post|put|patch|delete)\b|"
    r"\b(?P<bare_method>post|put|patch|delete)\b)",
    re.IGNORECASE,
)
ORDER_ENDPOINT = re.compile(
    r"/ord" r"ers?(?=[/?#\s\"'`]|$)",
    re.IGNORECASE,
)
PAREN_REQUEST_START = re.compile(
    r"(?:\.(?:(?:post|put|patch|delete|request)\s*\()|\bfetch\s*\()",
    re.IGNORECASE,
)
SHELL_REQUEST_START = re.compile(r"\bcurl\b", re.IGNORECASE)
MAX_REQUEST_EXPRESSION_LINES = 8

# Deliberate negative documentation is exempt only as an exact
# (relative path, matched text, reason) tuple. Splitting the string keeps the
# scanner's own source from resembling the forbidden endpoint example.
_DOCUMENTED_WRITE_METHOD = "PO" + "ST"
_DOCUMENTED_ORDER_ENDPOINT = "/ord" + "er"
ALLOWED_MATCHES = frozenset(
    {
        (
            "docs/publication-boundary.md",
            _DOCUMENTED_WRITE_METHOD + " " + _DOCUMENTED_ORDER_ENDPOINT,
            "external order-submission capability",
        ),
    }
)


def _public_files(root: Path) -> list[Path]:
    paths: list[Path] = []
    for relative_path in PUBLIC_ROOTS:
        scan_root = root / relative_path
        if scan_root.is_dir():
            paths.extend(scan_root.rglob("*"))
        elif scan_root.is_file():
            paths.append(scan_root)

    return sorted(
        path
        for path in paths
        if path.is_file()
        and not any(part in IGNORED_DIRECTORIES for part in path.relative_to(root).parts)
    )


def _method_token(match: re.Match[str]) -> str:
    return match.group("flag_method") or match.group("bare_method")


def _request_blocks(lines: list[str]) -> list[range]:
    blocks: list[range] = []
    start = 0
    while start < len(lines):
        line = lines[start]
        parenthesized = PAREN_REQUEST_START.search(line)
        if parenthesized:
            stop = min(len(lines), start + MAX_REQUEST_EXPRESSION_LINES)
            expression = line[parenthesized.start() :]
            balance = expression.count("(") - expression.count(")")
            end = start + 1
            while end < stop and balance > 0:
                balance += lines[end].count("(") - lines[end].count(")")
                end += 1
            blocks.append(range(start, end))
            start = end
            continue

        if SHELL_REQUEST_START.search(line):
            stop = min(len(lines), start + MAX_REQUEST_EXPRESSION_LINES)
            end = start + 1
            while end < stop and lines[end - 1].rstrip().endswith("\\"):
                end += 1
            blocks.append(range(start, end))
            start = end
            continue

        start += 1

    return blocks


def _http_order_matches(lines: list[str]) -> list[tuple[int, str]]:
    matches: list[tuple[int, str]] = []
    seen: set[tuple[int, str]] = set()

    def add_match(line_number: int, token: str) -> None:
        candidate = (line_number, token)
        if candidate not in seen:
            seen.add(candidate)
            matches.append(candidate)

    for line_number, line in enumerate(lines):
        method_match = HTTP_WRITE_METHOD.search(line)
        endpoint_match = ORDER_ENDPOINT.search(line)
        if method_match and endpoint_match:
            token_start = min(method_match.start(), endpoint_match.start())
            token_end = max(method_match.end(), endpoint_match.end())
            add_match(line_number, line[token_start:token_end])

    for block in _request_blocks(lines):
        method_matches = [
            (line_number, match)
            for line_number in block
            if (match := HTTP_WRITE_METHOD.search(lines[line_number]))
        ]
        endpoint_matches = [
            (line_number, match)
            for line_number in block
            if (match := ORDER_ENDPOINT.search(lines[line_number]))
        ]
        if not method_matches or not endpoint_matches:
            continue

        for endpoint_line, endpoint_match in endpoint_matches:
            method_line, method_match = min(
                method_matches,
                key=lambda candidate: abs(candidate[0] - endpoint_line),
            )
            if method_line == endpoint_line:
                line = lines[endpoint_line]
                token_start = min(method_match.start(), endpoint_match.start())
                token_end = max(method_match.end(), endpoint_match.end())
                token = line[token_start:token_end]
            else:
                token = f"{_method_token(method_match)} {endpoint_match.group(0)}"
            add_match(endpoint_line, token)

    return matches


def scan_repository(root: Path) -> list[Finding]:
    """Return forbidden-capability findings from all declared public surfaces."""

    root = root.resolve()
    findings: list[Finding] = []
    for path in _public_files(root):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except UnicodeDecodeError:
            continue

        relative_path = path.relative_to(root).as_posix()
        for line_number, line in enumerate(lines, start=1):
            for reason, pattern in FORBIDDEN.items():
                match = pattern.search(line)
                if match and (relative_path, match.group(0), reason) not in ALLOWED_MATCHES:
                    findings.append(Finding(relative_path, line_number, reason))

        reason = "external order-submission capability"
        for zero_based_line, token in _http_order_matches(lines):
            line_number = zero_based_line + 1
            allowed_match = (relative_path, token, reason)
            already_reported = any(
                finding.path == relative_path
                and finding.line == line_number
                and finding.reason == reason
                for finding in findings
            )
            if allowed_match not in ALLOWED_MATCHES and not already_reported:
                findings.append(Finding(relative_path, line_number, reason))

    return findings


def check_forbidden_surface() -> list[str]:
    """Preserve the original command-line check interface for existing callers."""

    return [
        f"{finding.path}:{finding.line}: {finding.reason}"
        for finding in scan_repository(ROOT)
    ]


def check_dashboard_script() -> list[str]:
    dashboard = ROOT / "crates" / "app" / "dashboard" / "index.html"
    body = dashboard.read_text(encoding="utf-8")
    start = body.find("<script>")
    end = body.rfind("</script>")
    if start < 0 or end <= start:
        return ["dashboard: missing inline script"]
    script = body[start + len("<script>") : end]
    with tempfile.NamedTemporaryFile("w", suffix=".js", encoding="utf-8") as handle:
        handle.write(script)
        handle.flush()
        result = subprocess.run(
            ["node", "--check", handle.name],
            capture_output=True,
            text=True,
            check=False,
        )
    return [] if result.returncode == 0 else [result.stderr.strip()]


def main() -> int:
    errors = check_forbidden_surface() + check_dashboard_script()
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("OK: paper-only boundary and dashboard syntax verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
