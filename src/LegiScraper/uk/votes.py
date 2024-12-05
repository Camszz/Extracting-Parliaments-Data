"""This module contains the Votes class that extracts information about UK Parliament votes."""

import requests as rq
import pandas as pd
from tqdm import tqdm
from time import sleep
import numpy as np
import time

from ..scraper import Scraper
from ..helpers import save_dataframe_to_folder
from ..topic_classifier import TopicAnalyzer

class Votes:

    def __init__(self,
                 config='base_uk-votes'
                 ):
        """Initialize the Votes class with the specified configuration."""

        self.scraper = Scraper(config=config)
        self.topic_analyzer = TopicAnalyzer(config=config)

    def run(self,):
        """Run the extraction and processing pipeline."""
       
        votes_to_extract = self.extract_votes().infer_objects()
        keywords, topics = self.topic_analysis(votes_to_extract.drop_duplicates(subset='Title'))
        topic_res = pd.concat([keywords, topics], axis=1).set_index('sequence')
        votes = votes_to_extract.set_index('Title').join(topic_res, validate='m:1').reset_index()
        save_dataframe_to_folder(votes, folder_path=self.scraper.config['output_folder'], file_name='votes.csv')

    def scrap_batch(self, batch):
        
        return pd.json_normalize(self.scraper.get_data(data_request='search', params={"skip": batch}))

    def extract_votes(self):

        n_votes = self.scraper.get_data(data_request='searchTotalResults')
        batches_id = np.arange(0, n_votes, 25)

        batch_res = list(map(self.scrap_batch, batches_id))

        votes_table = pd.concat(batch_res)

        cols_to_drop = ['Ayes', 'Noes', 'NoVoteRecorded']
        
        return votes_table.drop(columns=cols_to_drop)
    

    def topic_analysis(self, votes):
        """Perform topic analysis on the votes."""

        print(time.localtime())
        keywords = self.topic_analyzer.extract_keywords(votes['Title'])
        print(time.localtime())
        topics = self.topic_analyzer.topic_classifier(votes['Title'])
        print(time.localtime())

        return keywords, topics
