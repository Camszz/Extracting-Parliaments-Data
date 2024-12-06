# Extracting-Parliaments-Data
This project provides notebooks, an SQL database_eu as well as a preliminary data analysis of the EU Parliament (MEPs, votes, events, meetings, …). Objective is to integrate France and UK's parliaments.


# How to install this package ?
Create a conda environment using the following command : \
`conda create --name <env> --file requirements.txt` \
All required packages should be installed from the requirements.txt file.

Then, install the package in dev mode with the following command : \
`conda develop .`

# How to run this package ?

Start by creating a directory "notebooks" in the repository. In this directory, you can play with notebooks as much as you want without risking pushing the notebooks to the github repository.

In a notebook, do the following :

    from src.LegiScraper.database import Database

    db = Database(config='database')

Once you'll execute the above command, the Database class will automatically start scraping data while keeping you updated using the logger.