# glue/jobs/utils_bootstrap.py
"""sys.path bootstrapping so ``ods_pipeline`` is importable from spark workers.

Imported by ``utils.py`` (and the split utils_* siblings) to ensure the
project root and the canonical Glue container path are on ``sys.path``
before any ``ods_pipeline`` import.
"""
import os
import sys

_HERE = os.path.dirname(__file__)
for _root in (
    os.path.abspath(os.path.join(_HERE, "..", "..")),
    "/home/glue_user",
):
    if _root not in sys.path:
        sys.path.insert(0, _root)
