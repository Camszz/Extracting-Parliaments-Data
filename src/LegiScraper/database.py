"""This module contains the class that is responsible for creating the database and its necessary sub-datasets."""

import pandas as pd
from importlib import import_module
import logging

from .helpers import read_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:

    def __init__(self,
                 config : str,
                 ):
        
        self.config = read_config(config)
        self.create_datasets()
    
    def init_scraper(self, organization, data_key):

        logger.info(f"Initializing scraper for {organization} with data '{data_key}'")

        module_infos = self.config['data'][data_key]
        organization_infos = self.config['organizations'][organization]
        module_name = "src.LegiScraper." + organization_infos['module'] + "." + module_infos['module']

        class_name = module_infos['class']
        
        cls_gen = getattr(import_module(module_name), class_name)

        config_cls ='base_' + organization_infos['config'] + '-' + module_infos['config']

        return cls_gen(config=config_cls)
    
    def create_datasets(self):

        logger.info("Creating datasets")

        for org in self.config['organizations'].keys():
            for data_key in self.config['data'].keys():
                scraper = self.init_scraper(org, data_key)

                logger.info(f"Running scraper for {org} with data '{data_key}'")
                scraper.run()


