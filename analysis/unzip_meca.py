import os
from funcs.paths import paths
from simple_parsing import ArgumentParser

INPUT_DIR = paths["raw_data_dir"]
OUTPUT_DIR = paths["tmp_output"]


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "--input_file",
        type=str,
        help="input meca file relative to raw data dir",
    )
    args = parser.parse_args()
    assert args.input_file is not None

    input_file = INPUT_DIR / args.input_file
    assert input_file.exists(), input_file

    output_stem = input_file.stem
    output_file = OUTPUT_DIR / output_stem

    print(f"To extract {input_file} to {output_file}")
    cmd = f"unzip {input_file} -d {output_file}"
    stream = os.popen(cmd)
    output = stream.read()
    print(output)


if __name__ == "__main__":
    main()
