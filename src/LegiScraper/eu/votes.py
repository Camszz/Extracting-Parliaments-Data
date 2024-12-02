"""This module contains the Votes class that extracts information about EU Parliament votes."""

import requests as rq
import pandas as pd
from tqdm import tqdm
from time import sleep
import numpy as np

from ..scraper import Scraper
from .helpers import save_dataframe_to_folder
from ..topic_classifier import TopicAnalyzer

class Votes:

    def __init__(self,
                 config='base_eu-votes'
                 ):
        """Initialize the Votes class with the specified configuration."""

        self.scraper = Scraper(config=config)
        self.topic_analyzer = TopicAnalyzer(config=config)
    
    def run(self,):
        votes_to_extract = self.extract_votes()
        topics = self.topic_analysis(votes_to_extract)
        votes = pd.concat([votes_to_extract, topics], axis=1)
        save_dataframe_to_folder(votes, folder_path=self.scraper.config['output_folder'], file_name='votes.csv')

    def extract_votes(self,):
        df = pd.json_normalize(self.scraper.get_data()['results'])
        df = df.dropna(subset='reference')
        votes_to_extract = df[df['reference'].str.contains(r'[a-zA-Z]10-')][['id', 'timestamp', 'display_title', 'description', 'reference']]
        return votes_to_extract
    
    def topic_analysis(self, votes):
        keywords = self.topic_analyzer.extract_keywords(votes['display_title'])
        topics = self.topic_analyzer.topic_classifier(votes['display_title'])

        return pd.concat([keywords, topics], axis=1)

    