import pandas as pd    
import os


def get_mandate(data):

    mp_mandate = pd.json_normalize(data['hasMembership'])
    mp_eu_mandate = mp_mandate[mp_mandate['membershipClassification'] == 'def/ep-entities/EU_INSTITUTION']

    member_since = mp_eu_mandate['memberDuring.startDate'].min()
    last_membership = mp_eu_mandate['memberDuring.startDate'].max()
    
    if 'memberDuring.endDate' in mp_eu_mandate.columns:
        member_until = mp_eu_mandate['memberDuring.endDate'].dropna().max()
    else:
        member_until = None

    if member_until == None:
        return member_since, member_until
    else:
        if pd.to_datetime(member_until) < pd.to_datetime(last_membership):
            member_until = None

    return member_since, member_until

