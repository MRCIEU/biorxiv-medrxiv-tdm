from yiutils.project_utils import find_project_root

_root = find_project_root()

paths = {
    "root": _root,
    # input
    "data_root": _root / "data",
    "raw_data_dir": _root / "data" / "source-data",
    "examples_data_dir": _root / "data" / "source-data" / "examples",
    # output
    "output": _root / "output",
    "results": _root / "output" / "results",
    "artifacts": _root / "output" / "artifacts",
    "tmp_output": _root / "output" / "tmp",
}
