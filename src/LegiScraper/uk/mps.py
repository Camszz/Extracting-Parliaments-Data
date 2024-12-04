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
        
        return pd.concat(map(unpack_chunk, batch_res))

    def parallel_extract(self, ids):
        """
        Extract additional information about Members of Parliament in parallel.

        Args:
            ids (list): List of IDs for the MPs.

        Returns:
            dict: A dictionary where keys are indices and values are dictionaries
                containing detailed information about each MP.
        """
        # Use a list to gather results
        results = []

        # Create a multiprocessing pool
        with Pool(processes=os.cpu_count()) as pool:
            # Process IDs in parallel
            for result in tqdm(
                pool.imap_unordered(self.extract_add_infos, ids, chunksize=32),
                total=len(ids),
                desc="Obtaining MEP's Data"
            ):
                # Add a small sleep to avoid hitting the rate limiter
                sleep(np.random.uniform(0.5, 1))

                # Append the result to the results list
                results.append(result)

        # Convert results into a dictionary for final output
        outputs_dict = {
            i: {
                'id': r[0],
                'bday': r[1],
                'gender': r[2],
                'citizenship': r[3],
                'member_since': r[4],
                'member_until': r[5]
            } for i, r in enumerate(results)
        }

        return pd.DataFrame(outputs_dict).T.set_index('id')