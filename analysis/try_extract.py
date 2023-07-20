# TODO: extract key info from num_sample samples
#       - title; doi; publish year-month; authors;
#       - category
#       - ?? version
# from dataclasses import dataclass
# from simple_parsing import ArgumentParser
import json
from zipfile import ZipFile
from typing import Dict, Any
from pathlib import Path

import pandas as pd
from loguru import logger
from bs4 import BeautifulSoup as bs
from yiutils.failsafe import failsafe
from yiutils.processing import processing_wrapper
from pydash import py_
import xmltodict

from funcs.paths import paths

INPUT_DIR = paths["raw_data_dir"] / "medrxiv" / "Current_Content" / "March_2023"
assert INPUT_DIR.exists()
OUTPUT_DIR = paths["tmp_output"]
OUTPUT_FILE = OUTPUT_DIR / "tmp_results.json"
OUTPUT_ERROR_FILE = OUTPUT_DIR / "tmp_error.csv"

NUM_SAMPLE = 30


@failsafe
def main_extract(input_zip: Path) -> Dict[str, Any]:
    # read zip
    zip_path = str(input_zip.relative_to(paths["raw_data_dir"]))

    with ZipFile(input_zip, "r") as zip:

        # read manifest
        manifest_file = "manifest.xml"
        zip_data = zip.read(manifest_file)
        bs_content = bs(zip_data, "xml")
        find_res = bs_content.find_all("instance")
        full_text_file = find_res[0].attrs["href"].replace("\\", "/")

        # read fulltext
        zip_data = zip.read(full_text_file)
        bs_content = bs(zip_data, "xml")

    # fulltext
    fulltext = xmltodict.parse(str(bs_content))
    # title
    title = py_.chain(fulltext).at(
            ["article", "front", "article-meta", "article-title"]
            )

    res = {
        "zip_path": zip_path,
        "full_text_file": full_text_file,
        "title",
    }
    return res


def main():
    all_file_list = [_ for _ in INPUT_DIR.iterdir() if str(_).endswith(".meca")]
    logger.info(f"Num files in {INPUT_DIR} {len(all_file_list)}")
    sample_file_list = all_file_list[:NUM_SAMPLE]

    processing_res = [
        processing_wrapper(
            idx=idx,
            total=len(sample_file_list),
            payload={"input_zip": _},
            func=main_extract,
        )
        for idx, _ in enumerate(sample_file_list)
    ]

    main_res = [_[0] for _ in processing_res if _[0]]
    with OUTPUT_FILE.open("w") as f:
        json.dump(main_res, f)

    error_res = pd.DataFrame(
        [
            {"error": _[1], "context": _[2]}
            for _ in processing_res
            if isinstance(_[1], Exception)
        ]
    )
    error_res.to_csv(OUTPUT_ERROR_FILE, index=False)


if __name__ == "__main__":
    main()
