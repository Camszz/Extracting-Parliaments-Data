"""This module contains the class responsible for extracting and processing the MPs data."""

import pandas as pd
import os
from time import sleep
from tqdm import tqdm
from multiprocessing import Pool, Manager

from src.LegiScraper.scraper import Scraper
from .helpers import get_mandate


class MemberParliament:

    def __init__(self,
                 config='base'
                 ):
        """Initialize the MemberParliament object."""

        self.scraper = Scraper(config=config)
        self.params = {"format" : "application/ld+json"}

    def run(self,):
        """Run the extraction and processing pipeline."""

        df_mps = self.extract_mps()
        df_add_infos = self.parallel_extract(df_mps['id'])

        return df_add_infos

    def extract_mps(self,):

        json_data = self.scraper.get_data(mode='meps/show-current', params=self.params)
        
        df = pd.json_normalize(json_data['data'])
        df = df[['identifier', 'givenName', 'familyName', 'api:political-group', 'api:country-of-representation']]
        rename = {'identifier' : 'id',
          'givenName' : 'first_name',
          'familyName' : 'last_name',
          'api:political-group' : 'eu-parl-group',
          'api:country-of-representation' : 'country-representation'}
        df = df.rename(columns=rename)

        return df

    def parallel_extract(self, ids):

        with Manager() as manager:
            outputs_dict = manager.dict()

            with Pool(processes=os.cpu_count()) as pool:
                for i, result in tqdm(enumerate(pool.imap_unordered(self.extract_add_infos, ids, chunksize=8)), total=len(ids), desc="Obtaining MEP's Data"):
                    sleep(0.2)  # to avoid hitting the rate limiter

                    mp, bday, gender, citizenship, member_since, member_until = result
                    outputs_dict[i] = {'id': mp,
                                       'bday': bday,
                                       'gender': gender,
                                       'citizenship': citizenship,
                                       'member_since': member_since,
                                       'member_until': member_until}
        
        results_df = pd.DataFrame.from_dict(outputs_dict)

        return results_df

    
    def extract_add_infos(self, mp):
                
        mode = f'meps/{mp}'
        data = self.scraper.get_data(mode, self.params)['data'][0]
        bday = data['bday']
        gender = data['hasGender'].split('/')[-1]
        citizenship = data['citizenship'].split('/')[-1]

        member_since, member_until = get_mandate(data)

        return mp, bday, gender, citizenship, member_since, member_until
