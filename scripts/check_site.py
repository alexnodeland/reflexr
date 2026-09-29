"""Check the built documentation site for Markdown that rendered differently from GitHub.

GitHub starts a list without a blank line before it; Python-Markdown folds such a list into the
paragraph above it, so its items show up as text beginning with "- ". Nested lists are indented
by two spaces here, which the ``mdx_truly_sane_lists`` extension makes Python-Markdown accept; one
indented by more than its parent's text shows up as text too. Either way, a paragraph or list
item in the built site ends up containing a line that starts with a list marker. This script
finds them.

Usage: ``python scripts/check_site.py [site]``, after ``zensical build``.
"""

import html
import re
import sys
from pathlib import Path

_BLOCK = re.compile(r"<(p|li)\b[^>]*>(.*?)</\1>", re.S)
_CODE = re.compile(r"<pre\b.*?</pre>", re.S)
_SUBLIST = re.compile(r"<(?:ul|ol)\b.*", re.S)
_TAG = re.compile(r"<[^>]+>")
_MARKER = re.compile(r"\n\s*(?:[-*+]|\d+[.)])\s+\S")


def folded_lists(page: str) -> list[str]:
    """Return the start of each paragraph or list item in a page that has a list folded into it."""
    start, end = page.find("<article"), page.find("</article>")
    article = _CODE.sub("", page[start:end] if start >= 0 else page)
    found: list[str] = []
    for block in _BLOCK.finditer(article):
        text = html.unescape(_TAG.sub("", _SUBLIST.sub("", block[2])))
        if _MARKER.search(text):
            found.append(text.strip().splitlines()[0][:80])
    return found


def main(site: Path) -> int:
    """Print every folded list in the site, and return 1 if there are any."""
    problems = [
        f"{page.relative_to(site).parent}: {text}"
        for page in sorted(site.rglob("index.html"))
        for text in folded_lists(page.read_text())
    ]
    sys.stdout.writelines(f"{problem}\n" for problem in problems)
    if problems:
        sys.stderr.write(
            f"{len(problems)} list(s) rendered as text: put a blank line before each list, and "
            "indent nested items by their parent's text (two spaces after '-').\n"
        )
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1] if len(sys.argv) > 1 else "site")))
