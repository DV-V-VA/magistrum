import os, sys
here = os.path.abspath(os.path.dirname(__file__))
src = os.path.join(here, "src")
pkg = os.path.join(src, "longevity")
for p in (src, pkg):
    if os.path.isdir(p) and p not in sys.path:
        sys.path.insert(0, p)
