"""This module contains the Votes class that extracts information about EU Parliament votes."""

import requests as rq
import pandas as pd
from tqdm import tqdm
from time import sleep
import numpy as np


class Votes:

    def __init__(self,
                 config='base_eu'
                 ):
        """Initialize the Votes class with the specified configuration."""
