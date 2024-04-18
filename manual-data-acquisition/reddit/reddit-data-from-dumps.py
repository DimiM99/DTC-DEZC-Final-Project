import time
import zstandard
import os
import json
import csv
from datetime import datetime
import logging.handlers


input_file_path = "./data/source/CryptoCurrency_submissions.zst"
output_file_path = "./data/output/CryptoCurrency_submissions.csv"
fields = ["author","title","score","created","link","text","url"]

log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


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
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)
			if not chunk:
				break
			lines = (buffer + chunk).split("\n")

			for line in lines[:-1]:
				yield line, file_handle.tell()

			buffer = lines[-1]
		reader.close()


if __name__ == "__main__":
	start = time.time()
	file_size = os.stat(input_file_path).st_size
	file_lines, bad_lines = 0, 0
	line, created = None, None
	output_file = open(output_file_path, "w", encoding='utf-8', newline="")
	writer = csv.writer(output_file, escapechar='\\', quoting=csv.QUOTE_ALL)
	writer.writerow(fields)
	try:
		for line, file_bytes_processed in read_lines_zst(input_file_path):
			try:
				obj = json.loads(line)
				output_obj = []
				for field in fields:
					if field == "created":
						value = datetime.fromtimestamp(int(obj['created_utc'])).strftime("%Y-%m-%d %H:%M")
					elif field == "link":
						if 'permalink' in obj:
							value = f"https://www.reddit.com{obj['permalink']}"
						else:
							value = f"https://www.reddit.com/r/{obj['subreddit']}/comments/{obj['link_id'][3:]}/_/{obj['id']}/"
					elif field == "author":
						value = f"u/{obj['author']}"
					elif field == "text":
						if 'selftext' in obj:
							value = obj['selftext']
						else:
							value = ""
					else:
						value = obj[field]
					output_obj.append(str(value).encode("utf-8", errors='replace').decode())
				writer.writerow(output_obj)
				created = datetime.utcfromtimestamp(int(obj['created_utc']))
			except json.JSONDecodeError as err:
				bad_lines += 1
			file_lines += 1
			if file_lines % 100000 == 0:
				log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} File Lines : {bad_lines:,} Bad Lines : {(file_bytes_processed / file_size) * 100:.0f}%")
	except KeyError as err:
		log.info(f"Object has no key: {err}")
		log.info(line)
	except Exception as err:
		log.info(err)
		log.info(line)

	output_file.close()
	finish = time.time() - start
	log.info(f"Complete with : {file_lines:,} File Lines : {bad_lines:,} Bad Lines : in {finish:.2f} seconds")