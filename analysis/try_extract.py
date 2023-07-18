# TODO: extract key info from num_sample samples
#       - title; doi; publish year-month; authors;
#       - category
#       - ?? version
# from dataclasses import dataclass
# from simple_parsing import ArgumentParser

from zipfile import ZipFile
from typing import Dict, Any, Callable
from pathlib import Path

import pandas as pd
from loguru import logger
from bs4 import BeautifulSoup as bs
from yiutils.failsafe import failsafe

from funcs.paths import paths

INPUT_DIR = paths["raw_data_dir"] / "medrxiv" / "Current_Content" / "March_2023"
assert INPUT_DIR.exists()
OUTPUT_DIR = paths["tmp_output"]
OUTPUT_FILE = OUTPUT_DIR / "tmp.csv"

NUM_SAMPLE = 30


@failsafe
def main_extract(input_zip: Path) -> Dict[str, Any]:
    # read manifest
    manifest = "manifest.xml"
    with ZipFile(input_zip, "r") as zip:
        zip_data = zip.read(manifest)
    bs_content = bs(zip_data, "xml")
    find_res = bs_content.find_all("instance")
    full_text_file = find_res[0].attrs["href"]

    res = {"full_text_file": full_text_file}
    return res


def extract_wrapper(idx: int, total: int, payload: Dict[str, Any], func: Callable):
    echo_step = 10
    if idx % echo_step == 0:
        logger.info(f"#{idx}/{total}, payload: {payload}")
    fail_res = func(**payload)
    if fail_res[1]:
        res = fail_res[0]
    else:
        res = None
    return res


def main():
    all_file_list = [_ for _ in INPUT_DIR.iterdir() if str(_).endswith(".meca")]
    logger.info(f"Num files in {INPUT_DIR} {len(all_file_list)}")
    sample_file_list = all_file_list[:NUM_SAMPLE]

    sample_res = [
        extract_wrapper(
            idx=idx,
            total=len(sample_file_list),
            payload={"input_zip": _},
            func=main_extract,
        )
        for idx, _ in enumerate(sample_file_list)
    ]
    flatten_res = pd.DataFrame([_ for _ in sample_res if _ is not None])
    flatten_res.to_csv(OUTPUT_FILE, index=False)


if __name__ == "__main__":
    main()
