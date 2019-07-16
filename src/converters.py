import pandas as pd
import os

CONTEXT_HEADER = "Header"
CONTEXT_CONTENT = "LogFrame"
START = "Start"
END = "End"


def edats_to_df(file_directories, dst_directory):
    file_names = os.listdir(file_directories)
    column_names_from_headers = ['Subject', 'Age', 'Gender']

    content = list()
    for file_name in file_names:
        if not file_name.endswith(".txt"):
            continue
        print("converting " + file_name + "...")

        # add values from headers
        headers, content_ = edat_to_json(os.path.join(file_directories, file_name))
        for col_name in column_names_from_headers:
            if col_name in headers:
                for ele in content_:
                    ele[col_name] = headers[col_name]

        content.extend(content_)

    pd.Series(headers).to_csv(os.path.join(dst_directory, "headers.csv"))
    pd.DataFrame(content).to_csv(os.path.join(dst_directory, "content.csv"), index=False)


def edat_to_df(file_path, dst_directory):

    pf = open(file_path, 'r', encoding="utf-16-le")
    current_context = CONTEXT_HEADER

    headers = dict()
    content = list()

    raw_line = pf.readline()
    while len(raw_line) > 0:
        print(raw_line)
        raw_line = pf.readline()
        raw_line = raw_line.strip().replace("    ", "")

        if raw_line.startswith("*** " + CONTEXT_HEADER + " " + START):
            current_context = CONTEXT_HEADER
            continue
        if raw_line.startswith("*** " + CONTEXT_CONTENT + " " + START):
            current_context = CONTEXT_CONTENT
            content.append(dict())
            continue

        if len(raw_line) == 0 or raw_line.endswith("***"):
            continue

        key_, val_ = _line_to_row(raw_line, current_context)
        if current_context == CONTEXT_HEADER:
            headers[key_] = val_
        else:
            content[-1][key_] = val_

    pd.Series(headers).to_csv(os.path.join(dst_directory, "headers.csv"))
    pd.DataFrame(content).to_csv(os.path.join(dst_directory, "content.csv"))


def edat_to_json(file_path):
    pf = open(file_path, 'r', encoding="utf-16-le")
    current_context = CONTEXT_HEADER

    headers = dict()
    content = list()

    raw_line = pf.readline()
    while len(raw_line) > 0:
        raw_line = pf.readline()
        raw_line = raw_line.strip().replace("    ", "")

        if raw_line.startswith("*** " + CONTEXT_HEADER + " " + START):
            current_context = CONTEXT_HEADER
            continue
        if raw_line.startswith("*** " + CONTEXT_CONTENT + " " + START):
            current_context = CONTEXT_CONTENT
            content.append(dict())
            continue

        if len(raw_line) == 0 or raw_line.endswith("***"):
            continue

        key_, val_ = _line_to_row(raw_line, current_context)
        if val_ is None:
            continue
        if current_context == CONTEXT_HEADER:
            headers[key_] = val_
        else:
            content[-1][key_] = val_

    return headers, content


def _line_to_row(raw_line, context):

    splits = raw_line.split(": ", 2)
    if len(splits) != 2 and ":" not in raw_line:
        print(raw_line)
        raise ValueError()

    if len(splits) == 1:
        return splits[0], None
    return splits[0], splits[1]

