import requests
from bs4 import BeautifulSoup 
import pandas as pd 
import re
import json
from imdb import IMDb


def get_names_from_wiki():

    names = []
    languages = ['Tamil','Telugu','Kannada','Malayalam','Marathi','Bengali','Gujarati','Punjabi','Bollywood']

    for lang in languages:
        for year in range(2021,2022):

            wikiurl="https://en.wikipedia.org/wiki/List_of_" + str(lang) + "_films_of_" + str(year)
            print(wikiurl)   
            
            response=requests.get(wikiurl)

            soup = BeautifulSoup(response.text, 'html.parser')
            tbl = soup.find_all('table',{'class':"wikitable"})

            for i_tag in tbl:
                i_tag_element = i_tag.find_all('i')

                for name in i_tag_element:
                    names.append(name.text)

    names = list(set(names))

    return names


if __name__ == '__main__':
    imdb_obj = IMDb()

    names = get_names_from_wiki()
    print(len(names))