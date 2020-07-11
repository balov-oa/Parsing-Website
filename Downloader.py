import requests
import logging
import urllib
import sqlalchemy
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from typing import List, Set
from pathlib import Path
from datetime import datetime as dt


params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=OLEG;"
                                 "DATABASE=Apartment_Tomsk;"
                                 "Trusted_Connection=yes")
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={0}".format(params))


def get_soup_by_url(url: str) -> BeautifulSoup:
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'lxml')

    return soup


def get_number_last_page() -> int:
    soup = get_soup_by_url(
        'https://www.tomsk.ru09.ru/'
        'realty?type=1&otype=1&district[1]=on&district[2]=on&district[3]=on&district[4]=on&perpage=50&page=1')
    number_last_page = int(soup.find('td', {'class': 'pager_pages'}).find_all('a')[4].text)

    return number_last_page


def find_district_field(keys: List[str]) -> int:
    for i, j in enumerate(keys):
        if ' район' in j:
            return i


def parse_apartment(url: str) -> dict:
    soup = get_soup_by_url(url)

    keys = [i.find('span').text.replace('\xa0', '').lower() for i in
            soup.find_all('tr', {'class': 'realty_detail_attr'})]

    district_idx = find_district_field(keys)
    items = {'район': keys[district_idx]}

    keys = [j for i, j in enumerate(keys) if i not in (district_idx - 1, district_idx)]
    values = [i.text.replace('\xa0', ' ') for i in soup.find_all(class_='nowrap')]

    items.update(dict(zip(keys, values)))

    items['адрес'] = soup.find(class_='table_map_link').text.replace('\xa0', ' ')
    items['цена'] = int(
        soup.find('div', {'class': 'realty_detail_price inline'}).text.replace('\xa0', '').replace('руб.', ''))
    items['ид'] = int(soup.find('strong').text)
    items['дата добавления'] = soup.find(class_='realty_detail_date nobr').get('title')
    items['дата истечения'] = soup.find_all(class_='realty_detail_date')[4].get('title')
    items['ссылка'] = url

    return items


def get_urls_pages(start_page: int=1, end_page: int=None) -> Set[str]:
    url_base = 'https://www.tomsk.ru09.ru/' \
               'realty?type=1&otype=1&district[1]=on&district[2]=on&district[3]=on&district[4]=on&perpage=50&page='

    end_page = end_page or get_number_last_page()
    pages_to_parse = range(start_page, end_page + 1)
    urls_pages = [url_base + str(i) for i in pages_to_parse]

    return urls_pages


def get_urls_apartments_by_page(url_page: str) -> Set[str]:
    url_base = 'https://www.tomsk.ru09.ru'

    soup = get_soup_by_url(url_page)
    soup = soup.find_all('a', {'class': 'visited_ads'})

    urls_apartments = {url_base + i.get('href') for i in soup}

    return urls_apartments


def main(start_page: int=1, end_page: int=None) -> None:
    rename_map = {'район': 'District',
                  'адрес': 'Address',
                  'вид': 'Sales_Type',
                  'год постройки': 'Year_Building',
                  'материал': 'Material',
                  'этаж/этажность': 'Floor_Numbers_Of_Floors',
                  'этажность': 'Floors_In_Building',
                  'тип квартиры': 'Apartment_Type',
                  'цена': 'Price',
                  'общая площадь': 'Square_Total',
                  'жилая': 'Square_Living',
                  'кухня': 'Square_Kitchen',
                  'количество комнат': 'Rooms_Number',
                  'отделка': 'Apartment_Condition',
                  'санузел': 'Bathroom_Type',
                  'балкон/лоджия': 'Balcony_Loggia',
                  'дата добавления': 'Date_Add',
                  'дата истечения': 'Date_Expiration',
                  'ссылка': 'Url_Link',
                  'ид': 'Id'}

    df = pd.DataFrame(columns=list(rename_map.keys()))

    urls_in_database = pd.read_sql('SELECT DISTINCT Url_Link FROM Apartment_Tomsk.dbo.Apartments', engine)
    urls_in_database = set(urls_in_database['Url_Link'])

    len_storage = len(urls_in_database)
    print('Apartments in storage:', len_storage, '\n')
    logging.info('Apartments in storage: {0}'.format(len_storage))

    urls_pages = get_urls_pages(start_page, end_page)
    urls_apartments_to_parse = set()
    for url_page in tqdm(urls_pages, desc='Pages', leave=False, ascii=True):
        urls_apartments = get_urls_apartments_by_page(url_page)
        urls_apartments_to_parse.update(urls_apartments.difference(urls_in_database))

    if len(urls_apartments_to_parse) != 0:
        for url_apartment in tqdm(urls_apartments_to_parse, desc='Apartments', leave=False, ascii=True):
            df = df.append(parse_apartment(url_apartment), ignore_index=True)

    if not df.empty:
        df.rename(columns=rename_map, inplace=True)
        df['Download_timestamp'] = dt.now()
        df.to_sql(name='Apartments', con=engine, schema='dbo', if_exists='append', index=False)

    print('New Apartments:{0}'.format(len(df)))
    logging.info('New Apartments:{0}'.format(len(df)))


if __name__ == '__main__':
    log_file = Path(__file__).parent.joinpath('log.txt')
    logging.basicConfig(
        format="[%(asctime)s] -- %(levelname).3s -- %(message)s",
        datefmt='%Y.%m.%d %H:%M:%S',
        level=logging.INFO,
        filename=log_file)

    logging.info("Download start")
    try:
        main()
    except Exception as e:
        logging.error(e)
