# !/usr/bin/python3
# -*- coding: utf-8 -*-

# Script to scrape pages from ZeroHedge.com


# Imports
import concurrent.futures
import requests
from uuid import uuid4


# Configs
NUM_PAGES = 50  # 10 for refreshing current pages - e.g. viewcounts, comments, etc; > 5000 total number of pages as of 2017-10-24
URLS = ["http://www.zerohedge.com/"] + ["http://www.zerohedge.com/?page={}".format(_) for _ in range(1, NUM_PAGES)]
#OUT_PATH = "/home/ec2-user/zh_scraper/data/staging/"
OUT_PATH = "../data/staging/"

def load_url(url):
    # GET request for a single page. Returns response text.
    with requests.get(url) as resp:
        return resp.text


def run_spider(urls=URLS, out_path=OUT_PATH):
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
