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

# import html2text
from bs4 import BeautifulSoup as bs
from loguru import logger
from lxml import etree
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

NUM_SAMPLE = 800


def parse_full_text(
    full_text_dict: Dict[str, Any], full_text_bs: bs, verbose: bool = False
) -> Dict[str, Any]:
    # h = html2text.HTML2Text(bodywidth=0)

    # title
    title_find = (
        py_.chain(full_text_dict)
        .at(
            [
                "article",
                "front",
                "article-meta",
                "title-group",
                "article-title",
            ]
        )
        .value()
    )
    assert len(title_find) > 0
    title = title_find[0]
    if not isinstance(title, str):
        title_raw = full_text_bs.find_all("article-title")[0]
        tree = etree.fromstring(str(title_raw))
        title = str(etree.tostring(tree, method="text", encoding="utf-8"))
    assert isinstance(title, str), {"title": title}

    # article version
    version_find = (
        py_.chain(full_text_dict)
        .at(["article", "front", "article-meta", "article-version"])
        .value()
    )
    assert len(version_find) > 0
    version = version_find[0]
    assert isinstance(version, str), {"version": version}

    # category
    category_find = (
        py_.chain(full_text_dict)
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
        .value()
    )
    # NOTE: category could be None in rare cases
    # assert len(category_find) > 0
    category = category_find[0]
    # assert isinstance(category, str), {"category": category}

    # year-month

    res = {
        "title": title,
        "version": version,
        "category": category,
    }
    return res


@failsafe()
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
    verbose = False
    if (
        str(input_zip)
        == "/data/ik18445/projects/biorxiv-medrxiv-tdm/data/local-source-data/medrxiv/Current_Content/March_2023/dfeb991d-6c2f-1014-803d-aed9e05aecf4.meca"
    ):
        verbose = True
    full_text_dict = xmltodict.parse(str(bs_content))
    parse_res = parse_full_text(
        full_text_dict=full_text_dict, full_text_bs=bs_content, verbose=verbose
    )

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
            {"error": str(_[1]), "context": _[2]}
            for _ in processing_res
            if isinstance(_[1], Exception)
        ]
    )
    error_res.to_csv(OUTPUT_ERROR_FILE, index=False)


if __name__ == "__main__":
    main()
