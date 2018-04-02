#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Script to transform processed articles into dataframe and output as CSV
import logging
import pandas as pd
import json

# Set up logging
logger = logging.getLogger(__name__)

# Load processed articles data
with open("../data/processed/processed_articles", "r") as f:
    articles_raw = f.read().split("\n")
    articles = []  # list of article dictionaries
    for article in articles:
        try:
            articles.append(article)
        except Exception as e:
            logger.info("Could not process article")
            pass


# Create dataframe
article_df = pd.DataFrame.from_records(articles)

print(article_df.info(), "\n"*4)
print(article_df.head())
