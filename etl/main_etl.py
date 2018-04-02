#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# Script to run component ETL scripts - outputs processed df


import zh_spider
import zh_html_processor
import zh_dataframe_prep

import logging


# Configs
STAGING_DATA_PATH = "../data/staging/"
PROCESSED_DATA_PATH = "../data/processed/processed_articles"
MASTER_ARTICLE_DF_PATH = "../data/processed/master_article_df.csv"

# Set up logging
logger = logging.getLogger(__name__)


def run_etl(num_pages, staging_data_path, processed_data_path):	
    logger.info("Starting ETL process")

    # Scrape pages
    zh_spider.main(staging_data_path=staging_data_path, num_pages=num_pages)
    logger.info("Completed zh_spider")

    # Extract data from html pages
    zh_html_processor.main(staging_data_path=staging_data_path, processed_data_path=processed_data_path)
    logger.info("Completed zh_html_processor")

    zh_dataframe_prep.main(input_file_path=PROCESSED_DATA_PATH, output_file_path=OUT_FILE)
    logger.info("Completed zh_dataframe_prep")
    logger.info("ETL completed")


if __name__ == "__main__":
    run_etl(
    	num_pages=5,
    	staging_data_path=STAGING_DATA_PATH,
    	processed_data_path=MASTER_ARTICLE_DF_PATH)
