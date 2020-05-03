from os import listdir
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import json


def get_soup_by_url(url):
    html_ = requests.get(url).text
    soup = BeautifulSoup(html_, 'lxml')

    return soup


# Получаем номер последней страницы
def get_number_last_page():
    soup = get_soup_by_url(
        'https://www.tomsk.ru09.ru/realty?type=1&otype=1&district[1]=on&district[2]=on&district[3]=on&district[4]=on&perpage=50&page=1')
    number_last_page = int(soup.find('td', {'class': 'pager_pages'}).find_all('a')[4].text)

    return number_last_page


def find_district_field(keys):
    for i, j in enumerate(keys):
        if 'район' in j:
            break
    return i


def parse_apartment(url):
    #     headers = {'User-Agent: Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36'}
    #     start_time = time.time()

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

    return items


def get_urls_pages(start_page=1, end_page=None):
    url_base = 'https://www.tomsk.ru09.ru/realty?type=1&otype=1&district[1]=on&district[2]=on&district[3]=on&district[4]=on&perpage=50&page='

    end_page = end_page or get_number_last_page()
    pages_to_parse = range(start_page, end_page + 1)
    urls_pages = [url_base + str(i) for i in pages_to_parse]

    return urls_pages


def get_urls_apartments_by_page(url_page):
    url_base = 'https://www.tomsk.ru09.ru'

    soup = get_soup_by_url(url_page)
    soup = soup.find_all('a', {'class': 'visited_ads'})

    urls_apartments = set()

    for i in soup:
        urls_apartments.add(url_base + i.get('href'))

    return urls_apartments


def main(start_page=1, end_page=None, filename='data.json'):
    base_path = Path(__file__).parent

    if filename in listdir(base_path):
        with open(base_path.joinpath(filename), 'r') as file:
            storage_dict = json.load(file)
    else:
        storage_dict = {}

    len_storage = len(storage_dict)
    print('Apartments in storage:', len_storage, '\n')

    urls_pages = get_urls_pages(start_page, end_page)

    # for url_page in tqdm(urls_pages, desc='Pages', position=1, leave=False):
    for i, url_page in enumerate(urls_pages):

        urls_apartments = get_urls_apartments_by_page(url_page)
        urls_apartments_to_parse = urls_apartments.difference(set(storage_dict))
        print('Pages {0} of {1} - Count:{2}'.format(i, len(urls_pages), len(urls_apartments_to_parse)))

        if len(urls_apartments_to_parse) != 0:
            # for url_apartment in tqdm(urls_apartments_to_parse, desc='Apartments', position=0, leave=False):
            for j, url_apartment in enumerate(urls_apartments_to_parse):
                print('{0} / {1}'.format(j + 1, len(urls_apartments_to_parse)))

                storage_dict[url_apartment] = parse_apartment(url_apartment)

    with open(base_path.joinpath(filename), 'w') as file:
        json.dump(storage_dict, file, ensure_ascii=False)
    print('Done!')
    print('New apartments:', len(storage_dict) - len_storage)


if __name__ == '__main__':
    main()
