#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Script to transform processed articles into dataframe and output as CSV


import logging
import json

import pandas as pd
from textblob import TextBlob


# Set up logging
logger = logging.getLogger(__name__)


# ==============
# Load Data
# ==============
# Load processed articles data
with open("../data/processed/processed_articles", "r") as f:
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


# =====================
# Feature Engineering
# =====================
# 