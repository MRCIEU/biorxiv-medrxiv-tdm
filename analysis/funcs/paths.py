import warnings
from pathlib import Path

def _find_proj_root() -> Path:
    root = Path(".") / ".."
    anchor = root / "README.md"
    if not anchor.exists():
        warnings.warn("{anchor} not found".format(anchor=anchor.resolve()))
    return root

_root = _find_proj_root()
paths = {
    "root": _root,
    "data_root": _root / "data",
    "raw_data_dir": _root / "data" / "local-source-data",
    "examples_data_dir": _root / "data" / "local-source-data" / "examples",
}
