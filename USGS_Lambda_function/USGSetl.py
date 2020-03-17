import requests
import re
import psycopg2
from .config import *


CONN = psycopg2.connect(user=toyUSER,
                        password=toyPASSWORD,
                        host=toyHOST,
                        dbname=toyNAME,
                        port=5432)

HOUR = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'


def get_recent_quakes(url):
    '''This function gets all the quakes from USGS
    it takes in a url as an argument, which can be any of the gloabl
    variables in this package'''
    print('get_recent called')
    quakes = requests.get(url)
    print('requests.get complete ')
    quake_list = []
    for quake in quakes.json()['features']:
        quake_data = {}

        quake_data['Oceanic'] = bool(int(quake['properties']['tsunami']))
        quake_data['magnitude'] = quake['properties']['mag']
        quake_data['longitude'] = quake['geometry']['coordinates'][0]
        quake_data['latitude'] = quake['geometry']['coordinates'][1]
        quake_data['place'] = re.sub("[']", "''", quake['properties']['place'])
        quake_data['time'] = quake['properties']['time']
        print(f'quake {quake_data["place"]} added to list')
        quake_list.append(quake_data)
    print('quake_list complete')
    return quake_list


def insert_quakes(recents, period):
    '''this function takes in an extracted and transformed list of recent
    earthquakes and inserts them into the database, checking to make sure that
    the quake isn't already there. Once it finds a duplicate it stops inserting
    '''
    print('insertion called')
    curs = CONN.cursor()
    print('cursor created')
    last_time = get_last_times(recents[0]['time'], period)
    print('last quakes aquired')
    for quake in recents:
        if quake['time'] in last_time:
            print('old:', quake['place'], quake['Oceanic'])
        else:
            Place = quake['place']
            Time = quake['time']
            Latitude = quake['latitude']
            Longitude = quake['longitude']
            Magnitude = quake['magnitude']
            Oceanic = quake['Oceanic']
            insert_query = f"""INSERT INTO USGS
                        (Place, Time, Latitude, Longitude, Magnitude, Oceanic)
                        VALUES ('{Place}', {Time}, {Latitude}, {Longitude},
                        {Magnitude}, {Oceanic})"""
            curs.execute(insert_query)
            curs.close()
            CONN.commit()
            curs = CONN.cursor()
            print(f"new: {quake['place']}, {quake['Oceanic']} inserted")


def get_last_times(now, period='hour'):
    curs = CONN.cursor()
    times = []
    if period.upper() == 'HOUR' or period == HOUR:
        print('collecting one HOUR')
        curs.execute(f'SELECT time FROM USGS WHERE time >= {now-3.6e6}')
        time = curs.fetchall()
    elif period.upper() == 'DAY' or period == DAY:
        curs.execute(f'SELECT time FROM USGS WHERE time >= {now-8.64e7}')
        time = curs.fetchall()
    elif period.upper() == 'WEEK' or period == WEEK:
        curs.execute(f'SELECT time FROM USGS WHERE time >= {now-6.048e+8}')
        time = curs.fetchall()
    elif period.upper() == 'MONTH' or period == MONTH:
        curs.execute(f'SELECT time FROM USGS WHERE time >= {now-2.628e+9}')
        time = curs.fetchall()
    else:
        time = []

    for t in time:
        times.append(t[0])

    return times


def pipe_data(url):
    '''this function takes in a url, gets the recent earthquakes,
    then loads the new quakes into the database'''
    print('pipe called')
    recents = get_recent_quakes(url)
    insert_quakes(recents, url)
