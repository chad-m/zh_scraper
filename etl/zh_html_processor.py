#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# ETL script to load and extract data from raw html pages from www.zerohedge.com


# Imports
import bs4
import json
from pathlib import Path
import requests


# CONFIGS
DATA_PATH = "../data/staging"
OUT_FILE = "../data/processed/processed_articles"


def get_html_file_paths(folder_path=DATA_PATH, file_regex="*-*-*"):
    """
    Searches given folder path for raw zh html pages and returns file paths

    Params:
    -------
    folder_path (string): string of file path where html pages are stored
    file_regex (string): regular expression for files in folder_path

    Returns:
    --------
    file_paths (list: string): list of file paths for zh html pages

    """
    list_of_file_paths = list(Path(folder_path).glob(file_regex))
    file_paths = [str(_) for _ in list_of_file_paths]
    if file_paths:
        return file_paths
    else:
        print("No files in data folder. Check data folder. Exiting...")
        exit()


def extract_articles_from_file(raw_html):
    """
    Extracts list of articles from zh page html

    Params:
    -------
    raw_html (string): raw html text of zh page


    Returns:
    --------
    article_list (list: bs4.tag): list of articles from zh page

    """
    soup = bs4.BeautifulSoup(raw_html)
    article_list = soup.find("section", {"class": "article-list"}).findAll("article")
    if article_list:
        return article_list
    else:
        print("No articles found. Check input file and BeautifulSoup query")
        exit()


def extract_article_id(article):
    article_id = article.find(attrs={"entityid": True})["entityid"]
    return article_id


def extract_article_date_created(article):
    article_date_created = article.find(attrs={"created": True})["created"]
    return article_date_created


def extract_article_title(article):
    article_title = article.find("h2", {"class": "title"}).text
    return article_title


def extract_article_teaser_text(article):
    article_teaser_text = article.find(attrs={"class": "teaser-text"}).text
    return article_teaser_text


def extract_article_url(article):
    article_url = article.find("h2", {"class": "title"}).find("a").get("href")
    return article_url


def extract_article_views_count(article_id):
    """
    Gets count of times article has been viewed

    Params:
    -------
    article_id (string): unique id for zh article used for views data API query

    Returns:
    --------
    article_views_data (int): count of times article has been viewed

    """
    try:
        # Requires an additional HTTP GET requests using entityid as query arg.
        url = "http://counter.zerohedge.com/views?type=node&id=" + article_id
        resp = requests.get(url)
        tmp_data = resp.json()
        return tmp_data["entityStats"]["count"]
    except Exception as e:
        print("Article {} generated an exception: {}".format(article_id, e))
        print("Returning: NaN")
        return "NaN"


def extract_article_comments_count(article):
    try:
        article_comments_count = article.find(attrs={"class": "link-comments"}).text
        return article_comments_count
    except AttributeError:
        return "NaN"


def extract_data_from_article(article, filters=[
    extract_article_id,
    extract_article_date_created,
    extract_article_title,
    extract_article_teaser_text,
    extract_article_url,
    extract_article_views_count,
    extract_article_comments_count]):
    """
    Extracts data from article given a list of data filters

    Params:
    -------
    article (bs4.element.Tag): article html

    Returns:
    --------
    article_data (dict): dictionary in format {"article_id": val, "field1": value, "field2": value, ...}

    """
    article_data = {}
    for _ in filters:
        article_data[_.__name__.replace("extract_", "")] = _(article)
    return article_data


def process_html_file(file_to_process):
    """
    Loads given file and returns article data

    Params:
    -------
    file_to_process (string): File path of zh html page

    Returns:
    --------
    article_data (list): list of article data extracted from file: [{"article_id1": val, "field1": value, "field2": value, ...}, {"article_id2": _, ...}, ...]

    """
    with open(file_to_process, "rb") as f:
        raw_html = f.read()
    article_list = extract_articles_from_file(raw_html)
    article_data = [extract_data_from_article(_) for _ in article_list]
    return article_data


def output_processed_articles(processed_articles, out_file):
    """
    Appends a list of article data dicts to an output file location

    Params:
    -------
    processed_articles (list: dict): list of articles; each article is a dict(e.g. {"article_id: etc.})

    Returns:
    --------
    (None): appends each article in list to the given output file

    """
    # note: if file note created, create then append
    with open(out_file, "a") as f:
        for _ in processed_articles:
            f.write(json.dumps(_))
            f.write("\n")


# def delete_raw_file(raw_file):  # ONLY USE THIS AFTER TESTING
#     print("delete_raw_file not yet implemented")
#     exit()


def process_html_files(list_of_file_paths, out_file=OUT_FILE):
    # !!! This needs to be asyncronous !!!
    print("Processing zh files...")
    # Process zh html files, append article data to out file, and delete raw file from staging area
    for _ in list_of_file_paths:
        print("Processing file: {}".format(_))
        tmp_processed_articles = process_html_file(file_to_process=_)
        output_processed_articles(processed_articles=tmp_processed_articles, out_file=out_file)
        # delete_raw_file(file_to_delete=_)  # ONLY USE THIS AFTER TESTING
    print("Finished processing zh files.")


def main():
    zh_html_file_paths = get_html_file_paths(folder_path=DATA_PATH)
    process_html_files(list_of_file_paths=zh_html_file_paths, out_file=OUT_FILE)


if __name__ == "__main__":
    main()