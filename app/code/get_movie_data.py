import requests
from bs4 import BeautifulSoup 
import re
import json
import os
from imdb import IMDb, IMDbDataAccessError
import boto3 
from datetime import date

def process(spark, config):
    today_date = date.today() 
    current_year = today_date.year
    
    bucket_name = config['s3_bucket_details']['s3_bucket_data_path']
    data_folder_name = config['data']['data_folder_name']

    imdb_obj = IMDb()

    # loop through years
    # for year in range(current_year, current_year+1)
    for year in range(2001,2002):
        names = get_names_from_wiki(year)
        print(len(names))

        cleaned_names = clean_movie_list(names)
        print(len(cleaned_names))

        ids = find_unknown_id_movies(imdb_obj, cleaned_names)
        print(len(ids))

        movie_dict = create_dictionary(ids, cleaned_names)
        print(len(movie_dict))

        store_to_json(imdb_obj, movie_dict, year, bucket_name, data_folder_name)


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


def find_unknown_id_movies(imdb_obj, names):
    ids = []

    for movie in names:
        try:
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

        except IMDbDataAccessError:
            print("Operation timed out")

    return ids


def create_dictionary(ids, names):
    zip_obj = zip(ids, names)
    movie_dict = dict(zip_obj)

    if '0' in movie_dict: 
        del movie_dict['0']

    return movie_dict 


def store_to_json(imdb_obj, movie_dict, year, bucket_name, data_folder_name):

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
                        print(f"{attr} is unknown")

        except IMDbDataAccessError:
            print("Operation timed out")

        s3_client = boto3.client('s3')

        

        bucket = bucket_name
        bucket = bucket.split('/')[2:]
        bucket= '/'.join(bucket)

        key = data_folder_name + '/' + str(year) + '/' + str(id) + '.json' 

        print("Uploading file {key} to bucket {buck}".format(key=key, buck=bucket))

        s3_client.put_object(Body=(bytes(json.dumps(dict).encode('UTF-8'))), Bucket=bucket, Key=key)


