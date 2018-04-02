#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# Script to run component ETL scripts - outputs processed df


import zh_spider
import zh_html_processor
import zh_dataframe_prep

import logging


# Configs
STAGING_DATA_PATH = "../data/staging/"
PROCESSED_DATA_PATH = 

# Set up logging
logger = logging.getLogger(__name__)


def run_etl(num_pages):	
    logger.info("Starting ETL process")

    # Scrape pages
    zh_spider.run_spider(out_path=STAGING_DATA_PATH, num_pages=num_pages)
    logger.info("Completed zh_spider")    

    # Extract data from html pages
    zh_html_processor.main(staging_data_path=DATA_PATH, processed_data_path_and_file=OUT_FILE)
    logger.info("Completed zh_html_processor")

    zh_dataframe_prep.main()
    logger.info("Completed zh_dataframe_prep")
    logger.info("ETL completed")


if __name__ == "__main__":
    run_etl(num_pages=5)
