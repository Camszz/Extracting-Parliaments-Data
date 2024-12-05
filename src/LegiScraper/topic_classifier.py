import pandas as pd
from keybert import KeyBERT
from transformers import pipeline
import torch
from datasets import Dataset
from tqdm import tqdm

from.helpers import read_config, keywords_convert


class TopicAnalyzer:

    def __init__(self,
                 model=KeyBERT,
                 config : str = 'base'):
        
        self.model = model()
        self.params = read_config(config)
        self.init_classifier()

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

        keywords = self.model.extract_keywords(
            votes.tolist(),
            **params
        )

        keywords = keywords_convert(keywords)
        
        return keywords

    def init_classifier(self, model="MoritzLaurer/DeBERTa-v3-base-mnli-fever-anli"):
        if torch.backends.mps.is_available():
            device = torch.device("mps")
        elif torch.cuda.is_available():
            device = torch.device("gpu")
        else:
            device = -1
        self.classifier = pipeline("zero-shot-classification", model=model, device=device)

    def topic_classifier(self, votes):

        votes = votes.tolist()
        topics = self.params['topics']

        votes_topic = []

        for out in self.classifier(votes, topics, batch_size=32, multi_label=False):
            votes_topic.append(out)
        
        topics_res = [sentence['labels'][:2] for sentence in votes_topic]
        sequences = pd.DataFrame(votes_topic)['sequence']
        res = pd.DataFrame(topics_res, columns=['topic_1', 'topic_2'])
        return pd.concat([sequences, res], axis=1)


