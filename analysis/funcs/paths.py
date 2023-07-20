from yiutils.project_utils import find_project_root

_root = find_project_root()

paths = {
    "root": _root,
    # input
    "data_root": _root / "data",
    "raw_data_dir": _root / "data" / "local-source-data",
    "examples_data_dir": _root / "data" / "local-source-data" / "examples",
    # output
    "output": _root / "output",
    "tmp_output": _root / "output" / "tmp",
}
