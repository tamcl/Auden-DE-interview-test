import logging
import os

from src.MergingFunctions import (
    read_file,
    read_csv_to_dict,
    get_csv_files,
    index_key,
    merge_dict,
    dict_to_csv,
    write_to,
)

logging.basicConfig(
    format="%(levelname)s: %(asctime)s - %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
)

path = "./data/"
key = "ID"
output_path = "./output/output.csv"


def main():
    files = get_csv_files(path)
    merged_data = {}
    for i in files:
        logging.info(f"Start procesing {i}")
        byte_content = read_file(os.path.join(path, i))

        logging.info(f"Processing {i}")
        dictionary_data = read_csv_to_dict(byte_content)
        indexed_data = index_key(dictionary_data, key)
        merged_data[os.path.join(path, i)] = indexed_data
        logging.info(f"Process for {i} complete")

    logging.info("Merging dictionary")
    merged_files, merged_columns = merge_dict(list(merged_data.values()))

    logging.info("converting dictionary to csv")
    merged_file_content_string = dict_to_csv(merged_files, merged_columns)
    write_to(merged_file_content_string, output_path)


if __name__ == "__main__":
    main()
