from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import ray
from loguru import logger
from pydash import py_

from yiutils.processing import processing_wrapper, make_base_args  # isort:skip

from funcs.paths import paths  # isort:skip
from funcs import parse  # isort:skip

NUM_SAMPLES = 10
NUM_WORKERS = 8
OUTPUT_DIR = paths["results"] / "metadata_extract"


@ray.remote(num_cpus=1)
class RayWorker:
    def __init__(self, idx: int):
        self.idx = f"Worker {idx}"
        logger.info(f"{self.idx} ready.")

    def process(self, idx: int, item: Dict[str, Path]):
        prefix = "{worker} {stem}".format(
            worker=f"ray worker {self.idx}",
            stem=item["input_dir"].relative_to(paths["raw_data_dir"]),
        )
        logger.info(f"{self.idx}: Start processing item #{idx}: {prefix}.")
        extract(
            prefix=prefix,
            input_dir=item["input_dir"],
            output_results_file=item["output_results_file"],
            output_error_file=item["output_error_file"],
        )
        logger.info(f"{self.idx}: Finished processing item #{idx}.")

    def process_chunk(self, item_list: List[Dict[str, Path]]):
        logger.info(f"{self.idx}: Start processing.")
        [self.process(idx=idx, item=_) for idx, _ in enumerate(item_list)]
        logger.info(f"{self.idx}: Finished processing.")


def create_file_info(
    input_dir_list: List[Path], output_dir: Path
) -> pd.DataFrame:
    def create_paths(input_dir: Path, output_dir: Path) -> Dict[str, Path]:
        input_dir_rel_path = input_dir.relative_to(paths["raw_data_dir"])
        input_dir_stem = str(input_dir_rel_path).replace("/", "_")
        output_results_file = output_dir / f"{input_dir_stem}_results.csv"
        output_error_file = output_dir / f"{input_dir_stem}_errors.csv"
        res = {
            "input_dir": input_dir,
            "output_results_file": output_results_file,
            "output_error_file": output_error_file,
        }
        return res

    res = pd.DataFrame(
        [create_paths(_, output_dir=output_dir) for _ in input_dir_list]
    )
    return res


def extract(
    prefix: str,
    input_dir: Path,
    output_results_file: Path,
    output_error_file: Path,
) -> None:
    all_file_list = [
        _ for _ in input_dir.iterdir() if str(_).endswith(".meca")
    ]
    logger.info(f"{prefix}: Num files in {input_dir} {len(all_file_list)}")

    processing_res = [
        processing_wrapper(
            idx=idx,
            total=len(all_file_list),
            payload={"input_zip": _},
            func=parse.main_extract,
        )
        for idx, _ in enumerate(all_file_list)
    ]

    main_res = [_[0] for _ in processing_res if _[0]]
    main_res_df = pd.DataFrame(main_res)
    main_res_df.to_csv(output_results_file, index=False)

    error_res = pd.DataFrame(
        [
            {"error": str(_[1]), "context": _[2]}
            for _ in processing_res
            if isinstance(_[1], Exception)
        ]
    )
    error_res.to_csv(output_error_file, index=False)


def main():
    args = make_base_args()
    args.num_samples = NUM_SAMPLES
    print(args)

    OUTPUT_DIR.mkdir(exist_ok=True)

    # collect lv3 dirs under biorxiv and medrxiv lv1 and Current_Content Back_Content lv2
    input_dirs_nested = []
    for lv1_dir in ["biorxiv", "medrxiv"]:
        for lv2_dir in ["Back_Content", "Current_Content"]:
            lv2_path = paths["raw_data_dir"] / lv1_dir / lv2_dir
            lv3_dirs = [_ for _ in lv2_path.iterdir() if _.is_dir()]
            input_dirs_nested.append(lv3_dirs)
    input_dirs = py_.chain(input_dirs_nested).flatten().value()
    logger.info(f"All input_dirs {len(input_dirs)}")

    # set up output info
    meta_info = create_file_info(input_dirs, output_dir=OUTPUT_DIR)
    print(meta_info)
    meta_info.to_csv(OUTPUT_DIR / "meta_info.csv", index=False)

    sample_meta_info = (
        meta_info[: args.num_samples].to_dict(orient="records")
        if args.trial
        else meta_info.to_dict(orient="records")
    )
    logger.info(f"sample_meta_info {len(sample_meta_info)}")
    chunks = np.array_split(sample_meta_info, NUM_WORKERS)
    print([len(_) for _ in chunks])

    if not args.dry_run:
        # set up ray workers
        if ray.is_initialized():
            ray.shutdown()
        ray.init(num_cpus=NUM_WORKERS)
        logger.info(ray.available_resources())

        logger.info("processing init")
        workers = [
            RayWorker.remote(idx=idx)
            for idx, _ in enumerate(range(NUM_WORKERS))
        ]

        # real processing
        ray.get(
            [
                worker.process_chunk.remote(chunks[idx])
                for idx, worker in enumerate(workers)
            ]
        )


if __name__ == "__main__":
    main()
