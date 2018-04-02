#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# ETL script to load and extract data from raw html pages from www.zerohedge.com


# Imports
import bs4
import concurrent.futures
import json
import os
from pathlib import Path
import requests


# CONFIGS
#DATA_PATH = "/home/ec2-user/zh_scraper/data/staging/"
#OUT_FILE =  "/home/ec2-user/zh_scraper/data/processed/processed_articles"

DATA_PATH = "../data/staging/"
OUT_FILE = "../data/processed/processed_articles"


def get_html_file_paths(data_path=DATA_PATH, file_regex="*-*-*"):
    """
    Searches given folder path for raw zh html pages and returns file paths

    Params:
    -------
    data_path (string): string of file path where html pages are stored
    file_regex (string): regular expression for files in data_path

    Returns:
    --------
    file_paths (list: string): list of file paths for zh html pages

    """
    list_of_file_paths = list(Path(data_path).glob(file_regex))
    file_paths = [str(_) for _ in list_of_file_paths]
    if file_paths:
        return file_paths
    else:
        print("No files found in data folder. Check data folder: {}. Exiting...".format(data_path))
        exit()


def extract_articles_from_file(raw_html):
    """
    Extracts list of articles from a single zh page

    Params:
    -------
    raw_html (string): raw html text of zh page


    Returns:
    --------
    article_list (list: bs4.tag): list of articles from zh page

    """
    soup = bs4.BeautifulSoup(raw_html)
    article_list = soup.find("div", {"id": "block-zerohedge-content"}).findAll("article")
    if article_list:
        return article_list
    else:
        print("No articles found. Check input file and BeautifulSoup query. Skipping...")


def extract_article_id(article):
    # article_id = article.find(attrs={"entityid": True})["entityid"]  # previous
    article_id = article.find(attrs={"data-entityid": True})["data-entityid"]
    return article_id


def extract_article_date_created(article):
    # article_date_created = article.find(attrs={"created": True})["created"]  # previous
    article_date_created = article.find(attrs={"class": "extras__created"}).find("span").text
    return article_date_created


def extract_article_title(article):
    # article_title = article.find("h2", {"class": "title"}).text  # previous
    article_title = article.find(attrs={"property":"schema:name"}).text
    return article_title


def extract_article_teaser_text(article):
    # article_teaser_text = article.find(attrs={"class": "teaser-text"}).text  # also works
    article_teaser_text = article.find(attrs={"property": "schema:text"}).find("p").text  # cleaner text
    return article_teaser_text


def extract_article_url(article):
    article_url = article.find("h2", {"class": "teaser-title"}).find("a").get("href")
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
    # Check article_id
    assert isinstance(article_id, str), "article_id not type string. type is: {}".format(type(article_id))
    try:
        # Requires an additional HTTP GET requests using entityid as query arg.
        url = "https://www.zerohedge.com/statistics-ajax?entity_ids=" + article_id
        resp = requests.get(url)
        tmp_data = resp.json()
        return tmp_data[article_id]
    except Exception as e:
        print("Article {} generated an exception: {}. Returning NaN".format(article_id, e))
        return "NaN"


# def extract_article_comments_count(article):  # NEEDS UPDATED bs4 selector
#     try:
#         # article_comments_count = article.find(attrs={"class": "link-comments"}).text
#         print(extract_article_comments_count)
#         return article_comments_count
#     except AttributeError:
#         return "NaN"


def extract_data_from_article(article, filters=[
    extract_article_id,
    extract_article_date_created,
    extract_article_title,
    extract_article_teaser_text,
    extract_article_url,
    extract_article_views_count,
    # extract_article_comments_count
    ]):
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
        tmp_key = _.__name__.replace("extract_", "")
        if "views_count" in tmp_key:
            article_data[tmp_key] = _(article_data["article_id"])
        else:
            article_data[tmp_key] = _(article)
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
    article_data = []
    for _ in article_list:
        try:
            article_data.append(extract_data_from_article(_))
        except Exception as e:
            print("Article data not extracted for {}... Error \n{}".format(file_to_process, e))
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
    with open(out_file, "a") as f:
        for _ in processed_articles:
            f.write(json.dumps(_))
            f.write("\n")


def process_html_files(list_of_file_paths, out_file=OUT_FILE):
    # Process list of zh page pages (multithreaded)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(process_html_file, _): _ for _ in list_of_file_paths}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                tmp_processed_articles = future.result()
                output_processed_articles(processed_articles=tmp_processed_articles, out_file=OUT_FILE)
                # delete file - note: will be implemented at later date
            except Exception as e:
                print('{} generated an exception: {}'.format(url, e))
            else:
                print('{} page processed.'.format(url))


def main(staging_data_path=DATA_PATH, processed_data_path=OUT_FILE):
    # Delete previous processed files
    try:
        os.remove(staging_data_path)
    except Exception as e:
        pass

    # Load paths of scraped html files
    zh_html_file_paths = get_html_file_paths(data_path=staging_data_path)
    
    # Extract and output data from processed html file
    process_html_files(list_of_file_paths=zh_html_file_paths, out_file=processed_data_path)


if __name__ == "__main__":
    main()
