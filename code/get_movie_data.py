import requests
from bs4 import BeautifulSoup 
import pandas as pd 
import re
import json
import os
from imdb import IMDb, IMDbDataAccessError
from utils import load_config

def get_names_from_wiki(year):

    names = []
    languages = ['Tamil','Telugu','Kannada','Malayalam','Marathi','Bengali','Gujarati','Punjabi','Bollywood']

    for lang in languages:
        wikiurl="https://en.wikipedia.org/wiki/List_of_" + str(lang) + "_films_of_" + str(year)
        print(wikiurl)   
            
        response=requests.get(wikiurl)

        soup = BeautifulSoup(response.text, 'html.parser')
        tbl = soup.find_all('table',{'class':"wikitable"})

        for i_tag in tbl:
            i_tag_element = i_tag.find_all('i')

            for name in i_tag_element:
                names.append(name.text)

    return names


def clean_movie_list(names):
    names = list(set(names))

    while "" in names:
        names.remove("")

    return names


def find_unknown_id_movies(names):
    ids = []

    #imdb_obj = IMDb()

    for movie in names:
        search = imdb_obj.search_movie(movie)

        if not search: 
            ids.append('0')
            #print("ID not found")

        elif search[0]['title'].lower() == movie.lower():
            id = search[0].getID()
            ids.append(id)
            #print("ID is found")

        else:
            ids.append('0')
            #print("ID not found")

    return ids


def create_dictionary(ids, names):
    zip_obj = zip(ids, names)
    movie_dict = dict(zip_obj)

    if '0' in movie_dict: 
        del movie_dict['0']

    return movie_dict 


def create_sub_folder(folder_name, year):
    path = '../' + str(folder_name) + '/' + str(year)
   
    if not os.path.exists(path):
        os.makedirs(path)


def store_to_json(movie_dict, year):
    #imdb_obj = IMDb()

    for id in movie_dict.keys():
        try:
            movie = imdb_obj.get_movie(id)
            keys = movie.keys()
        
            dict = {}

            dict['id'] = id 

            for attr in keys:
                try:
                    if type(movie.data[attr]) == str:
                        dict[attr] = movie.data[attr]
                
                    if type(movie.data[attr]) == type(list) or type(movie.data[attr]) == list:
                        if len(movie.data[attr]) == 1:
                            dict[attr] = str(movie.data[attr][0])
                        else:    
                            attr_list = movie.data[attr]
                            element_list = [] 

                            for i in range(len(attr_list)):
                                element_list.append(str(attr_list[i]))
                                dict[attr] = element_list
                            
                except KeyError:
                        #print(f"{attr} is unknown")
                        pass

        except IMDbDataAccessError:
            print("error")

        file_name = '../data/' + str(year) + '/' + str(id) + '.json'  
        
        with open(file_name, 'w') as file:
            json.dump(dict, file, indent=4)
        

if __name__ == '__main__':

    config_data = load_config() 

    folder_name = config_data['data']['folder_name']

    imdb_obj = IMDb()
    # loop through years
    for year in range(2001,2002):
        names = get_names_from_wiki(year)
        print(len(names))

        cleaned_names = clean_movie_list(names)
        print(len(cleaned_names))

        ids = find_unknown_id_movies(cleaned_names)
        print(len(ids))

        movie_dict = create_dictionary(ids, cleaned_names)
        print(len(movie_dict))

        create_sub_folder(folder_name, year)

        store_to_json(movie_dict, year)