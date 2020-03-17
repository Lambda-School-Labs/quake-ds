from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import time
from .DBQueries import *


CREATE_EMSC = '''CREATE TABLE EMSC
                (ID SERIAL PRIMARY KEY,
                place TEXT,
                time TEXT,
                lat FLOAT,
                lon FLOAT,
                mag FLOAT)'''


def setup_EMSC(pages):
    curs = CONN.cursor()
    curs.execute('DROP TABLE EMSC')
    curs.execute(CREATE_EMSC)
    fill_db(pages)
    curs.close()
    CONN.commit()

def get_last_day():
    url = 'https://www.emsc-csem.org/Earthquake/?view='
    page_num = 2
    today = True
    page_insert = ""
    while(today):
        page = requests.get(url+str(page_num), timeout=5)
        page_soup = BeautifulSoup(page.text, 'html.parser')
        table = page_soup.find('tbody')
        rows = table.find_all('tr')
        for row in rows:
            try:
                cells = row.find_all('td')
                time = pd.to_datetime(row.find(class_="tabev6")
                                      .find('a').text)
                lat = float(cells[4].text) if cells[5].text.strip(
                    '\xa0') == 'N' else -float(cells[4].text)
                lon = float(cells[6].text) if cells[7].text.strip(
                    '\xa0') == 'E' else -float(cells[6].text)
                mag = float(cells[10].text)
                place = re.sub("'", "''", cells[11].text.strip('\xa0'))
                row_insert = f"('{place}', '{time}', {lat}, {lon}, {mag}), "
                page_insert += row_insert
                page_num += 1
                print(row_insert)
            except:
                today = False
                break




def get_table(i):
    url = 'https://www.emsc-csem.org/Earthquake/?view='+str(i)
    try:
        page = requests.get(url, timeout=5)
    except:
        print(f'request {i} timed out, trying again')
        return get_table(i)
    page_soup = BeautifulSoup(page.text, 'html.parser')
    try:
        table = page_soup.find('tbody')
        rows = table.find_all('tr')
    except:
        print(f'request {i} returned no table, trying again')
        return get_table(i)
    page_insert = 'INSERT INTO EMSC (place, time, lat, lon, mag) VALUES '
    for row in rows:
        try:
            cells = row.find_all('td')
            time = pd.to_datetime(row.find(class_="tabev6")
                                  .find('a').text)
            lat = float(cells[4].text) if cells[5].text.strip(
                '\xa0') == 'N' else -float(cells[4].text)
            lon = float(cells[6].text) if cells[7].text.strip(
                '\xa0') == 'E' else -float(cells[6].text)
            mag = float(cells[10].text)
            place = re.sub("'", "''", cells[11].text.strip('\xa0'))
        except:
            print('passed row')
        row_insert = f"('{place}', '{time}', {lat}, {lon}, {mag}), "
        page_insert += row_insert

    return page_insert[:-2]+';'


def fill_db(pages):
    curs = CONN.cursor()
    for i in range(1, pages+1):
        print(i)
        query = get_table(i)
        curs.execute(query)
        curs.close()
        CONN.commit()
        curs = CONN.cursor()
        print(f'query {i} completed')
