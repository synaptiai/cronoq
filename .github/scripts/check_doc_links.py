"""Check relative links and #anchors in Markdown files.

Usage: python3 .github/scripts/check_doc_links.py <file-or-dir> [...]

External (http/https/mailto) links are not checked. Anchors use the GitHub/mdBook heading-id
rules (lowercase, punctuation removed, spaces to dashes). Exits non-zero on broken links.
"""

import os
import re
import sys

CODE = re.compile(r"```.*?```", re.S)
HEADING = re.compile(r"^#+\s+(.*)$", re.M)
LINK = re.compile(r"\]\(([^)\s]+)\)")


def slug(heading: str) -> str:
    h = re.sub(r"<[^>]+>", "", heading).strip().lower()
    h = re.sub(r"[^\w\- ]", "", h)
    return h.replace(" ", "-")


def markdown_files(paths):
    for p in paths:
        if os.path.isdir(p):
            for d, _, files in os.walk(p):
                for f in files:
                    if f.endswith(".md"):
                        yield os.path.normpath(os.path.join(d, f))
        else:
            yield os.path.normpath(p)


def main(paths) -> int:
    files = list(markdown_files(paths))
    texts = {p: CODE.sub("", open(p, encoding="utf-8").read()) for p in files}
    anchors = {p: {slug(h) for h in HEADING.findall(t)} for p, t in texts.items()}
    broken = 0
    for p, text in texts.items():
        for link in LINK.findall(text):
            if link.startswith(("http://", "https://", "mailto:")):
                continue
            path, _, frag = link.partition("#")
            target = os.path.normpath(os.path.join(os.path.dirname(p), path)) if path else p
            if not os.path.exists(target):
                print(f"{p}: missing file {link}")
                broken += 1
            elif frag and target in anchors and frag not in anchors[target]:
                print(f"{p}: missing anchor {link}")
                broken += 1
    print(f"checked {len(files)} files, {broken} broken links")
    return 1 if broken else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:] or ["."]))
