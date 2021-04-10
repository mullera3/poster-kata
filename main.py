import os
from insert import InsertDB
import requests
import json

def parse_films_data(json_data,ship):
    # parse json data and returns a dict and a chunk of the json from the url
    row_dict = {}
    for k in json_data.keys():
        if k == "results":
            ships = json_data[k]
    
    temp = None
    for i in ships:
        for j in i.keys():
            if j == "name":
                if i[j] == ship:
                    row_dict[j] = i[j]
                    temp = i

    return temp, row_dict


def get_film_urls(film_data,row_dict):
    #loops through list of films urls and grabs data
    #pass data to function which parse it and creates a dict
    for i in film_data:
        url = i
        r = requests.get(url)
        api_data = json.dumps(r.json())
        data = json.loads(api_data)
        join_data(data,row_dict)



            

def join_data(film_urls,row_dict):
    #joins the data from api with poster content from other db
    #parses data and creates and dict which keys store into database by key-pair value
    title = ""
    episode_id = 0
    director = ''
    release_date = ''
    for key in film_urls.keys():
        if key == "title":
            row_dict[key] = film_urls[key]
        elif key == "episode_id":
            row_dict[key] = int(film_urls[key])
        elif key == "director":
            row_dict[key] = film_urls[key]
        elif key == "release_date":
            row_dict[key] = film_urls[key]

    #inserting info into database
    db2.insert_into_films(row_dict["title"], row_dict["episode_id"], row_dict["director"], row_dict["release_date"],row_dict["name"])

def grabFilmUrls(ship):
    #grabs films list for specific ship 
    films = None
    for i in ship.keys():
        if i == "films":
            films = ship[i]

    return films


if __name__ == "__main__":

    #database info
    hostname = "localhost"
    user = "postgres"
    password = "postgres"
    d1 = "source"
    d2 = "dw"

    global db 
    db = InsertDB(hostname, user, password, d1)
    global db2
    db2 = InsertDB(hostname, user, password, d2)
    #
    poster_table = db.show_table_data("posters")
    ship_names = db.filter_posters_table()

    db2.create_dw_films_table()

    url = "http://swapi.dev/api/starships/"
    r = requests.get(url=url)
    api_data = json.dumps(r.json())
    data = json.loads(api_data)

    for i in ship_names:
        ship_json,ship_dict = parse_films_data(data,i)
        film_urls = grabFilmUrls(ship_json)
        get_film_urls(film_urls,ship_dict)

    print(db2.show_table_data("films"))
    
