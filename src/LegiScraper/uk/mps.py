"""This module contains the class responsible for extracting and processing the MPs data."""

import pandas as pd
import os
import numpy as np
from time import sleep
from tqdm import tqdm
from multiprocessing import Pool

from ..scraper import Scraper
from ..helpers import save_dataframe_to_folder
from .helpers import unpack_chunk

class MemberParliament:

    def __init__(self,
                 config='base_mps'
                 ):
        """Initialize the MemberParliament object."""

        self.scraper = Scraper(config=config)
        self.params = self.scraper.config['params']

    def run(self,):
        """Run the extraction and processing pipeline."""

        df = self.extract_mps()
        save_dataframe_to_folder(df, folder_path=self.scraper.config['output_folder'], file_name='mps_data.csv')

    def scrap_batch(self, batch):
        return self.scraper.get_data(data_request='Search', params={"skip": batch})

    def extract_mps(self,):

        n_mps = self.scraper.get_data(data_request='Search')['totalResults']
        batches_id = np.arange(0, n_mps, 20)

        batch_res = list(map(self.scrap_batch, batches_id))

        mp_table = pd.concat(map(unpack_chunk, batch_res))
        
        to_drop = mp_table.columns[mp_table.columns.str.contains('Colour')]
        mp_table = mp_table.drop(columns=to_drop)
        
        return mp_table
