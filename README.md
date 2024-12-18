# Extracting-Parliaments-Data
This project provides notebooks, an SQL database_eu as well as a preliminary data analysis of the EU Parliament (MEPs, votes, events, meetings, …). Objective is to integrate France and UK's parliaments.


# How to install this package ?
All required packages will be installed from the requirements.txt file automatically.

First, create a environment with python 3.12.7. You can do create one using conda : \
`conda create -n <env_name> python=3.12.7`

Then, activate the environment : \
`conda activate <env_name>`

Install the package in dev mode with the following command : \
`python -m pip install -e .`

And then, if you use a conda environment, to make sure the package is well loaded by conda, run the following command: \
`conda develop .`

# How to run this package ?

Start by creating a directory "notebooks" in the repository. In this directory, you can play with notebooks as much as you want without risking pushing the notebooks to the github repository.

In a notebook, do the following :

    from src.LegiScraper.database import Database

    db = Database(config='database')

Once you'll execute the above command, the Database class will automatically start scraping data while keeping you updated using the logger.