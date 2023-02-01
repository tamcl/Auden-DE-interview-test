import logging
from typing import ByteString, Optional
import os
import re


def get_csv_files(path):
    logging.info(f"Search CSV files in {path}")
    return [x for x in os.listdir(path) if ".csv" in x]


def read_file(path):
    logging.info(f"Reading file {path}")
    with open(path, "rb") as f:
        content = f.read()
    return content


def read_csv_to_dict(content: bytes, header=True, regex_string = r'"([^"]*)"|([^,]+)'):
    content_string = content.decode("utf-8")
    row_datas = content_string.split("\n")
    logging.debug("Finding header column")
    if header:
        column_header = [
            x[0] or x[1] for x in re.findall(regex_string, row_datas[0])
        ]
        row_datas = row_datas[1:]
    else:
        column_header = list(
            range(
                len(
                    [
                        x[0] or x[1]
                        for x in re.findall(regex_string, row_datas[0])
                    ]
                )
            )
        )
    data = []
    logging.debug("Converting string content to dictionary")
    for row_data in row_datas:
        if row_data:
            row_content = [
                x[0] or x[1] for x in re.findall(regex_string, row_data)
            ]
            data.append(dict(zip(column_header, row_content)))
    return data


def index_key(data: list[dict], key: str):
    output_data = {}
    for d in data:
        tmp_value = {k: v for k, v in d.items() if k != key}
        tmp_key = d.get(key)
        if tmp_key in output_data.keys():
            raise ValueError(f"Duplicate key found {tmp_key}")
        output_data[tmp_key] = tmp_value
    return output_data


def merge_dict(listOfData: list[dict]):
    logging.info("Merging dictionary")
    output_dict = {}
    columns = set()
    for data in listOfData:
        for k, v in data.items():
            if k not in output_dict.keys():
                output_dict[k] = dict()
            columns.update(set(v.keys()))
            clash_column = [i for i in v.keys() if i in output_dict.keys()]
            if len(clash_column) == 0:
                output_dict[k].update(v)
            else:
                raise ValueError(f"Duplicate column names {clash_column}")
    columns = list(columns)
    columns.sort()
    return output_dict, columns


def dict_to_csv(dict_data, column):
    output_csv_content = "," + ",".join(column) + "\n"
    for k, v in dict_data.items():
        tmp_row_content = [v.get(c, None) for c in column]
        row_content = [f'"{k}"']
        for c in tmp_row_content:
            if isinstance(c, str):
                row_content.append(f'"{c}"')
            elif c is None:
                row_content.append("")
            else:
                row_content.append(f"{c}")
        output_csv_content += ",".join(row_content) + "\n"
    return output_csv_content


def write_to(content: str, path):
    logging.info(f'Saving content to {path}')
    try:
        os.mkdir(os.path.dirname(path))
    except FileExistsError as e:
        logging.error(repr(e))
    with open(path, "w") as f:
        f.write(content)
