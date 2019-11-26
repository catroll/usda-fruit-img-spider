#!/usr/bin/env python3

import concurrent.futures
import functools
import logging
import os
from http.client import HTTPConnection

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(pathname)s %(message)s')
HTTPConnection.debuglevel = 1
requests_log = logging.getLogger('requests.packages.urllib3')
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

CACHE_FOLDER = 'page_caches/'
IMG_FOLDER = 'fruit_images/'
EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=5)
FUTURES = []
PAGE_COUNT = 380


def parse_page(page):
    logging.info('[page %s] start parse page', page)
    cache_file = os.path.join(CACHE_FOLDER, f'page-{page}.html')
    if os.path.exists(cache_file):
        logging.debug('[page %s] cache hit', page)
        with open(cache_file) as _file:
            page_html = _file.read()
    else:
        url = f'https://usdawatercolors.nal.usda.gov/pom/search.xhtml'
        params = {'start': (page - 1) * 20, 'searchText': '', 'searchField': '', 'sortField': ''}
        resp = requests.get(url, params=params)
        page_html = resp.text
        with open(cache_file, 'w') as _file:
            _file.write(page_html)
    soup = BeautifulSoup(page_html, 'html.parser')
    for (div_idx, div) in enumerate(soup.select('div.document')):
        doc = div.select_one('dl.defList')
        artist = doc.select_one(':nth-child(2)>a').get_text()
        year = doc.select_one(':nth-child(4)>a').get_text()
        # cannot parse scientific name or common name for some pictures, just use 'none' instead to avoid terminating
        scientific_name = 'none' if doc.select_one(':nth-child(6)>a') is None \
            else doc.select_one(':nth-child(6)>a').get_text()
        common_name = 'none' if doc.select_one(':nth-child(8)>a') is None \
            else doc.select_one(':nth-child(8)>a').get_text()
        thumb_pic_src = div.select_one('div.thumb-frame>a>img')['src']
        id = (page) * 20 + div_idx + 1
        info = FruitInfo(id, artist, year, scientific_name, common_name, thumb_pic_src)
        logging.info('[page %s] %s', page, info)
        FUTURES.append(EXECUTOR.submit(info.download_and_save))


class FruitInfo:
    def __init__(self, id, artist, year, scientific_name, common_name, thumb_pic_url):
        self.id = id
        self.artist = artist
        self.year = year
        self.scientific_name = scientific_name
        self.common_name = common_name
        self.thumb_pic_url = thumb_pic_url

    def download_and_save(self):
        filename = f'{self.id}-{self.common_name}-{self.year}-{self.artist}.jpg'.replace(' ', '_')
        filepath = os.path.join(IMG_FOLDER, filename)
        if os.path.exists(filepath):
            logging.info('[%s] filename = %s, exists!', self.id, filename)
            return
        logging.info('[%s] filename = %s', self.id, filename)
        ori_img_url = self.__parse_ori_img_url()
        logging.info('[%s] original img url = %s', self.id, ori_img_url)
        resp = requests.get(ori_img_url)
        with open(filepath, 'wb') as f:
            f.write(resp.content)
            logging.info('[%s] %s saved...', self.id, filename)

    def __parse_ori_img_url(self) -> str:
        img_id = self.thumb_pic_url.split('/')[2]
        logging.info('[%s] img id = %s', self.id, img_id)
        return f'https://usdawatercolors.nal.usda.gov/pom/download.xhtml?id={img_id}'

    def __str__(self):
        return (f'FruitInfo(artist={self.artist},year={self.year},scientific_name={self.scientific_name},'
                f'common_name={self.common_name},thumb_pic_url={self.thumb_pic_url})')


def main():
    for i in range(PAGE_COUNT):
        parse_page(i + 1)
        concurrent.futures.wait(FUTURES)
        for index, future in enumerate(FUTURES):
            if future.done():
                FUTURES.pop(index)


if __name__ == '__main__':
    main()
