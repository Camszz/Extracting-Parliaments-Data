import pandas as pd



def unpack(data):
    return pd.json_normalize(data['value'], sep='_')

def unpack_chunk(chunk):
    return pd.concat(map(unpack, chunk['items']))