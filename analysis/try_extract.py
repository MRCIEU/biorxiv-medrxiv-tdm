import pandas as pd
from loguru import logger
from pydash import py_

from yiutils.processing import processing_wrapper  # isort:skip

from funcs.paths import paths  # isort:skip
from funcs import parse  # isort:skip

INPUT_DIR = (
    # paths["raw_data_dir"] / "medrxiv" / "Current_Content" / "March_2023"
    paths["raw_data_dir"]
    / "biorxiv"
    / "Current_Content"
    / "April_2019"
)
assert INPUT_DIR.exists()
OUTPUT_DIR = paths["tmp_output"]
OUTPUT_RESULTS_FILE = OUTPUT_DIR / "tmp_results.csv"
OUTPUT_ERROR_FILE = OUTPUT_DIR / "tmp_error.csv"

NUM_SAMPLE = 800


def main():
    all_file_list = [
        _ for _ in INPUT_DIR.iterdir() if str(_).endswith(".meca")
    ]
    logger.info(f"Num files in {INPUT_DIR} {len(all_file_list)}")
    if len(all_file_list) == 0:
        logger.info(f"No files in {INPUT_DIR}")
        lv4_dirs = [_ for _ in INPUT_DIR.iterdir() if _.is_dir()]
        logger.info(f"Num lv4 dirs in {INPUT_DIR} {len(lv4_dirs)}")
        all_file_list = (
            py_.chain(
                [
                    __
                    for _ in lv4_dirs
                    for __ in _.iterdir()
                    if str(__).endswith(".meca")
                ]
            )
            .flatten()
            .value()
        )

    sample_file_list = all_file_list[:NUM_SAMPLE]

    processing_res = [
        processing_wrapper(
            idx=idx,
            total=len(sample_file_list),
            payload={"input_zip": _},
            func=parse.main_extract,
        )
        for idx, _ in enumerate(sample_file_list)
    ]

    main_res = [_[0] for _ in processing_res if _[0]]
    main_res_df = pd.DataFrame(main_res)
    main_res_df.to_csv(OUTPUT_RESULTS_FILE, index=False)

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
