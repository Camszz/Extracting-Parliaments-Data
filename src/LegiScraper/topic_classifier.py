import pandas as pd
from keybert import KeyBERT
from transformers import pipeline
import torch

from.helpers import read_config, keywords_convert


class TopicAnalyzer:

    def __init__(self,
                 votes,
                 model=KeyBERT,
                 config : str = 'base'):
        
        self.votes = votes
        self.model = model()
        self.params = read_config(config)

    # Function to extract keywords using KeyBERT
    def extract_keywords(self, votes, params=None):
        """
        Extracts keywords from the given descriptions using KeyBERT.
        
        Parameters:
        - votes: List of votes diescriptions
        - params: Parameters for the keywords extraction
        
        Returns:
        - List of extracted keywords
        """

        if params is None:
            params = self.params['keybert_params']
            params['use_mmr'] = params['use_mmr'] == 'True'
            params['keyphrase_ngram_range'] = tuple(params['keyphrase_ngram_range'])

        keywords_list = self.model.extract_keywords(
            votes,
            **params
        )

        keywords_df = keywords_convert(keywords_list)
        
        return keywords_df