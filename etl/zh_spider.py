# !/usr/bin/python3
# -*- coding: utf-8 -*-

# Script to scrape pages from ZeroHedge.com


# Imports
import concurrent.futures
import requests
from uuid import uuid4


# Configs
#OUT_PATH = "/home/ec2-user/zh_scraper/data/staging/"
OUT_PATH = "../data/staging/"

def load_url(url):
    # GET request for a single page. Returns response text.
    with requests.get(url) as resp:
        return resp.text

def clear_staging_files(staging_path=OUT_PATH):
    # Delete previous scraped files
    try:
        os.remove(staging_path)
    except Exception as e:
        pass


def run_spider(out_path=OUT_PATH, num_pages=2):
    # Clear previously scraped files
    clear_staging_files(out_path)

    # Build urls - max number > 5000
    urls = ["http://www.zerohedge.com/"] + ["http://www.zerohedge.com/?page={}".format(_) for _ in range(1, num_pages)]

    # Writes raw HTML response results to output path (multithreaded)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(load_url, url): url for url in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            outf = str(uuid4())  # random file name
            try:
                data = future.result().encode("utf-8")
                with open(out_path + outf, "w") as f:
                    f.write(str(data))
            except Exception as e:
                print('{} generated an exception: {}'.format(url, e))
            else:
                print('{} page is {} bytes'.format(url, len(data)))


if __name__ == "__main__":
    run_spider()
