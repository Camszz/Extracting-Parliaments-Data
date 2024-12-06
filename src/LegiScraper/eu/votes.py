"""This module contains the Votes class that extracts information about EU Parliament votes."""

import requests as rq
import pandas as pd
from tqdm import tqdm
from time import sleep
import numpy as np

from ..scraper import Scraper
from ..helpers import save_dataframe_to_folder
from ..topic_classifier import TopicAnalyzer

class Votes:

    def __init__(self,
                 config='base_eu-votes'
                 ):
        """Initialize the Votes class with the specified configuration."""

        self.scraper = Scraper(config=config)
        self.topic_analyzer = TopicAnalyzer(config=config)
    
    def run(self,):
        votes_to_extract = self.extract_votes().infer_objects()
        keywords, topics = self.topic_analysis(votes_to_extract.drop_duplicates(subset='display_title'))
        topic_res = pd.concat([keywords, topics], axis=1).set_index('sequence')
        votes = votes_to_extract.set_index('display_title').join(topic_res, validate='m:1').reset_index()
        df_mp_votes = self.mp_votes(votes)
        save_dataframe_to_folder(votes, folder_path=self.scraper.config['output_folder'], file_name='votes_eu.csv')
        save_dataframe_to_folder(df_mp_votes, folder_path=self.scraper.config['output_folder'], file_name='member_votes_eu.csv')

    def extract_votes(self,):
        df = pd.json_normalize(self.scraper.get_data()['results'])
        df = df.dropna(subset='reference')
        votes_to_extract = df[df['reference'].str.contains(r'[a-zA-Z]10-')][['id', 'timestamp', 'display_title', 'description', 'reference']]
        return votes_to_extract
    
    def topic_analysis(self, votes):
        keywords = self.topic_analyzer.extract_keywords(votes['display_title'])
        topics = self.topic_analyzer.topic_classifier(votes['display_title'])

        return keywords, topics
    
    def mp_votes(self, votes_to_extract):

        df = []

        for id in tqdm(votes_to_extract['id']):
            data_request = f"/{id}"
            vote = self.scraper.get_data(data_request=data_request)
            df_membervotes_id = pd.json_normalize(vote['member_votes'])[['member.id','position']]
            df_membervotes_id.loc[:, 'vote-id'] = id
            df.append(df_membervotes_id)
            sleep(0.5)
        
        df = pd.concat(df)
        df.rename(columns={'member.id' : 'member-id'}, inplace=True)
        
        return df