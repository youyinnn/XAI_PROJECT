#!/usr/bin/env python
import os
import sys
from pathlib import Path
from data_process.data_extraction.extract_arxiv_data import extract_by_cate as ebc

def main():
    base_dir = str(Path(__file__).resolve().parent)
    data_dir = os.path.join(base_dir, "data_process", "data")
    os.environ.setdefault("BASE_DIR", base_dir)
    os.environ.setdefault("DATA_DIR", data_dir)

    # print(os.environ.get("DATA_DIR"))
    argv_len = len(sys.argv)
    if argv_len > 1:
        cmd = sys.argv[1]
        if (cmd == 'extract-cate' and argv_len >= 3):
            cate = sys.argv[2]
            ebc(cate, os.path.join(data_dir, "arxiv-metadata-oai-snapshot.data"))

if __name__ == '__main__':
    main()