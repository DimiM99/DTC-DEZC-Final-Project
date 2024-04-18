import io

import pandas as pd
import time
import zstandard
import os
import json
import csv
from datetime import datetime
import logging.handlers

from pandas import DataFrame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
    chunk = reader.read(chunk_size)
    bytes_read += chunk_size
    if previous_chunk is not None:
        chunk = previous_chunk + chunk
    try:
        return chunk.decode()
    except UnicodeDecodeError:
        if bytes_read > max_window_size:
            raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
        return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name):
    with open(file_name, 'rb') as file_handle:
        buffer = ''
        reader = zstandard.ZstdDecompressor(max_window_size=2 ** 31).stream_reader(file_handle)
        while True:
            chunk = read_and_decode(reader, 2 ** 27, (2 ** 29) * 2)
            if not chunk:
                break
            lines = (buffer + chunk).split("\n")

            for line in lines[:-1]:
                yield line, file_handle.tell()

            buffer = lines[-1]
        reader.close()


@data_loader
def load_data_from_file(*args, **kwargs):
    input_file_path = "/home/src/data/source/CryptoCurrency_submissions.zst"
    output_dir = "/home/src/data/output/CryptoCurrency_submissions"
    output_file_path = f"{output_dir}/reddit_dump_data_"
    fields = ["author", "title", "score", "created", "link", "text", "url"]

    log = logging.getLogger("bot")
    log.setLevel(logging.DEBUG)
    log.addHandler(logging.StreamHandler())

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    
    batch_number = 0
    decompressor = zstandard.ZstdDecompressor()
    with open(input_file_path, 'rb') as compressed:
        with decompressor.stream_reader(compressed) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')
            file_lines, bad_lines = 0, 0
            batch_size = 100000
            batch_data = []
            for line in text_stream:
                file_lines += 1
                try:
                    obj = json.loads(line)
                    output_obj = {field: None for field in fields}
                    for field in fields:
                        if field == "created":
                            output_obj[field] = datetime.fromtimestamp(int(obj['created_utc'])).strftime("%Y-%m-%d %H:%M")
                        elif field == "link":
                            if 'permalink' in obj:
                                output_obj[field] = f"https://www.reddit.com{obj['permalink']}"
                            else:
                                output_obj[field] = f"https://www.reddit.com/r/{obj['subreddit']}/comments/{obj['link_id'][3:]}/_/{obj['id']}/"
                        elif field == "author":
                            output_obj[field] = f"u/{obj['author']}"
                        elif field == "text":
                            output_obj[field] = obj.get('selftext', "")
                        else:
                            output_obj[field] = obj[field]
                    batch_data.append(output_obj)
                    if len(batch_data) >= batch_size:
                        df = pd.DataFrame(batch_data)
                        df.to_parquet(f'{output_file_path}batch_{batch_number}.parquet', index=False)
                        batch_number += 1
                        batch_data = []
                        print(f"Processed {file_lines:,} lines. Bad lines: {bad_lines:,}. Batch: {batch_number}")
                except json.JSONDecodeError:
                    bad_lines += 1
                    continue
    print("Files saved to output dir")
    results = DataFrame()
    for i in range(batch_number):
        print(f"Retrieving batch {i}")
        df = pd.read_parquet(f'{output_file_path}batch_{i}.parquet')
        results = pd.concat([results, df])
    print("returning")
    return results
