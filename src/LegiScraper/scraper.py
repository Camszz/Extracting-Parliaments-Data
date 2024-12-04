"""This file contains the scrapper object that is responsible for scrapping the data using APIs."""

import requests as rq
import pandas as pd
import json
from tqdm import tqdm
from time import sleep

from .helpers import read_config


class Scraper:
    
    def __init__(self,
                 config='base'):
        """Initialize the scraper object with the provided config."""
        
        self.config = read_config(config)

    def get_data(self,
                 data_request : str = '',
                 params: dict = {}):
        """Basic function designed to request a specific data ('data_request') from
        the EU parliament API with parameters ('params') in the request.
        It returns the JSON-formatted response body."""
        
        url = self.config['url'] + data_request
        headers = self.config['headers']

        if params == {}:
            params = self.config['params']

        response = rq.get(url=url, params=params, headers=headers)
        try :
            return response.json()
        except :
            print(f'Error : here is the response the system got @ {url} : {response}')

    