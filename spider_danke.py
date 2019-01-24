import requests
from bs4 import BeautifulSoup
import redis
import re
import time
import random
import pymongo

MONGO_DB = "danke_test"

# 获取城市地点和网站链接
def get_locations(rds):
    url = 'https://www.dankegongyu.com/room/bj/d%E6%B5%B7%E6%B7%80%E5%8C%BA-b%E4%BA%94%E9%81%93%E5%8F%A3.html'
    responese = requests.get(url=url, headers=headers)
    soup = BeautifulSoup(responese.text, 'lxml')
    locations = soup.find('dl', class_='dl_lst list subway').find_all('div', class_='area-ls-wp')
    # 获取地区的url
    for location in locations:
        all_hrefs = location.find('div', class_='sub_option_list').find_all('a')
        for href in all_hrefs:
            key = href.text.strip()
            rds.hset('danke_hash', key, href['href'])    #把地铁站和链接以使用hash存储
            rds.rpush('danke_name', key)    #存储地铁站入redis列表

# 存入mongodb数据库
def save_to_mongo(total, client):
    db = client["ffzs"]
    collection = db[MONGO_DB]
    collection.insert_one(total)

# 获取每个地区的租房信息
def get_info(url, client):
    while True:
        responese = requests.get(url=url, headers=headers)
        soup = BeautifulSoup(responese.text, 'lxml')
        if soup.find('div', class_='r_ls_box'):
            houses = soup.find('div', class_='r_ls_box').find_all('div', class_='r_lbx')
            for house in houses:
                xiaoqu = house.find('div', class_='r_lbx_cena').find('a')
                house_url = xiaoqu['href']
                house_info = list(filter(None, xiaoqu['title'].split(' ')))   #空格分割并去除列表中的空值
                house_info.append(house_url)
                size = house.find('div', class_='r_lbx_cenb').get_text(strip=True)
                size_list = [x.strip() for x in size.split('|')]
                house_info.extend(size_list[:2])
                house_info.append(size_list[-1][:-1])
                house_info.append(size_list[-1][-1])
                if house.find('div', class_='room_price'):
                    new_price = re.sub('[ \n]', '', house.find('div', class_='new-price-link').get_text(strip=True))[
                                :-2]
                    house_price = house.find('div', class_='room_price').get_text()
                else:
                    new_price = ''
                    house_price = house.find('div', class_='r_lbx_moneya').get_text()
                house_info.append(new_price)
                price = ''.join(list(filter(str.isdigit, house_price)))     #字符串只保留数字为住房价格
                house_info.append(price)
                tags = house.find('div', class_='r_lbx_cenc').get_text('\n', strip=True).split('\n')
                house_info.append(tags)
                distance_subway = house.find('div', class_='r_lbx_cena').get_text(strip=True).split("站")[-1][:-1]
                print(distance_subway)
                house_info.append(distance_subway)
                total = {}
                for i, info in enumerate(house_info):
                    total[house_head[i]] = info
                save_to_mongo(total, client)
                print('成功存入mongo：{}'.format(total))

            time.sleep(random.choice([1, 1.5, 2]))
            pages = soup.find('div', class_='page').find_all('a')
            if pages[-1].get_text(strip=True) == '>':    # 判断是否有下一页
                url = pages[-1]['href']
            else:
                break
        else:
            break

if __name__ == '__main__':
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
        'Referer': 'https://www.dankegongyu.com/room/bj'

    }
    client = pymongo.MongoClient("mongodb://localhost:27017", connect=False)
    pool = redis.ConnectionPool.from_url('redis://:666666@localhost:6379', db=1, decode_responses=True)
    r = redis.Redis(connection_pool=pool)
    get_locations(r)
    house_head = ['location', 'community', 'type', 'url', 'area', 'floor', 'face', 'the_way', 'new_price', 'price', 'tags', 'distance_subway']
    while True:
        key = r.lpop('danke_name')
        if key:
            url = r.hget('danke_hash', key)
            print(url)
            get_info(url, client)
        else: break



