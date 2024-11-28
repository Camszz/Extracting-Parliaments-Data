"""This file contains the scrapper object that is responsible for scrapping the data using APIs."""

import requests as rq
import pandas as pd
import json
from tqdm import tqdm
from time import sleep


class Scraper:
    
    def __init__(self,
                 config='base'):
        """Initialize the scraper object with the provided config."""
        
        self.config = self.get_config(config)

    def get_config(self,
                   config):
        """Load the configuration file and return the corresponding settings."""
        
        try:
            with open(f'../config/{config}.json', 'r') as file:
                config = json.load(file)
            return dict(config)
        except FileNotFoundError:
            print(f'Error: Config file {config}.json not found.')
            return None

    def EUPARL_getJSON(self,
                       mode : str,
                       params : dict = {}) :
        """Basic function designed to request a specific data ('mode') from the EU parliament API with parameters ('params') in the request.
        It returns the JSON-formatted response body."""
        
        url = self.config['url'] + mode
        response = rq.get(url=url, params=params)
        try :
            return response.json()
        except :
            print(f'Error : here is the response the system got @ {url} : {response}')

    