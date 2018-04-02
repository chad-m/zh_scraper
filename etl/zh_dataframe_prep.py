#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Script to transform processed articles into dataframe and output as CSV


import logging
import json

import pandas as pd
from textblob import TextBlob


# Set up logging
logger = logging.getLogger(__name__)


# Configs
INPUT_FILE = "../data/processed/processed_articles"
OUT_FILE = "../data/processed/master_article_df.csv"


def main():
    # Load processed articles data
    with open(INPUT_FILE, "r") as f:
        articles_raw = f.read().split("\n")
        articles = []  # list of article dictionaries
        for article in articles_raw:
            try:
                articles.append(json.loads(article))
            except Exception as e:
                logger.info("Could not process article")
                pass


    # Create article dataframe
    article_df = pd.DataFrame.from_records(articles)

    # Extract article title sentiment polarity and subjectivity
    article_title_sentiment = pd.Series(article_df['article_title'].map(lambda _: TextBlob(_).sentiment))

    # Article title polarity column
    article_df["article_title_polarity"] = article_title_sentiment.map(lambda _: _[0])

    # Article title subjectivity
    article_df["article_title_subjectivity"] = article_title_sentiment.map(lambda _: _[1])

    # Output to csv
    article_df.to_csv(OUT_FILE, index=False)


if __name__ == "__main__":
    main()
