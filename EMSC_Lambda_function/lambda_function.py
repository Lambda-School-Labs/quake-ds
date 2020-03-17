import json
import psycopg2
from bs4 import BeautifulSoup
import requests
import re
from config import *


CONN = psycopg2.connect(user=toyUSER,
                        password=toyPASSWORD,
                        host=toyHOST,
                        dbname=toyNAME,
                        port=5432)

def query(query):
    curs = CONN.cursor()
    try:
        curs.execute(query)
    except:
        print('Query error')
    curs.close()
    CONN.commit()

def get_last_day():
    '''collects the last day of earthquakes'''
    url = 'https://www.emsc-csem.org/Earthquake/?view='
    page_num = 2
    today = True
    page_insert = 'INSERT INTO EMSC (place, time, lat, lon, mag) VALUES '
    while(today):
        page = requests.get(url+str(page_num), timeout=5)
        page_soup = BeautifulSoup(page.text, 'html.parser')
        table = page_soup.find('tbody')
        rows = table.find_all('tr')
        for row in rows:
            if row['class'][0] != 'autour':
                cells = row.find_all('td')
                time = row.find(class_="tabev6").find('a').text
                lat = float(cells[4].text) if cells[5].text.strip(
                    '\xa0') == 'N' else -float(cells[4].text)
                lon = float(cells[6].text) if cells[7].text.strip(
                    '\xa0') == 'E' else -float(cells[6].text)
                mag = float(cells[10].text)
                place = re.sub("'", "''", cells[11].text.strip('\xa0'))
                row_insert = f"('{place}', '{time}', {lat}, {lon}, {mag}), "
                page_insert += row_insert
            else:
                today = False
                break
        page_num += 1
    return page_insert[:-2]+';'

def lambda_handler(event, context):
    query(get_last_day())
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
