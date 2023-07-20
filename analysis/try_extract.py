# TODO: extract key info from num_sample samples
#       - title; doi; publish year-month; authors;
#       - category
#       - ?? version
# from dataclasses import dataclass
# from simple_parsing import ArgumentParser
import json
from pathlib import Path
from typing import Any, Dict
from zipfile import ZipFile

import pandas as pd
import xmltodict
from bs4 import BeautifulSoup as bs
from loguru import logger
from pydash import py_

from yiutils.failsafe import failsafe  # isort:skip
from yiutils.processing import processing_wrapper  # isort:skip

from funcs.paths import paths  # isort:skip

INPUT_DIR = (
    paths["raw_data_dir"] / "medrxiv" / "Current_Content" / "March_2023"
)
assert INPUT_DIR.exists()
OUTPUT_DIR = paths["tmp_output"]
OUTPUT_FILE = OUTPUT_DIR / "tmp_results.json"
OUTPUT_ERROR_FILE = OUTPUT_DIR / "tmp_error.csv"

NUM_SAMPLE = 30


def parse_full_text(full_text: Dict[str, Any]) -> Dict[str, Any]:
    # title
    title = (
        py_.chain(full_text)
        .at(
            [
                "article",
                "front",
                "article-meta",
                "title-group",
                "article-title",
            ]
        )
        .value()[0]
    )
    assert isinstance(title, str)

    # article version
    version = (
        py_.chain(full_text)
        .at(["article", "front", "article-meta", "article-version"])
        .value()[0]
    )
    assert isinstance(version, str)

    # category
    category = (
        py_.chain(full_text)
        .at(
            [
                "article",
                "front",
                "article-meta",
                "article-categories",
                "subj-group",
                "subject",
            ]
        )
        .value()[0]
    )
    assert isinstance(category, str)

    res = {
        "title": title,
        "version": version,
        "category": category,
    }
    return res


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
    full_text = xmltodict.parse(str(bs_content))
    parse_res = parse_full_text(full_text)

    res = {
        "zip_path": zip_path,
        "full_text_file": full_text_file,
        "title": parse_res["title"],
        "version": parse_res["version"],
        "category": parse_res["category"],
    }
    return res


def main():
    all_file_list = [
        _ for _ in INPUT_DIR.iterdir() if str(_).endswith(".meca")
    ]
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
