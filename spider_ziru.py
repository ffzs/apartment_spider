import requests
from bs4 import BeautifulSoup
import redis
import re
import time
import random
import pymongo
import pytesseract
from PIL import Image
from urllib import request

MONGO_DB = "ziru_test"

# 获取地铁站点和链接
def get_location(rds):
    url = 'http://www.ziroom.com/z/nl/z2.html'
    response = requests.get(url, headers=headers).text
    soup = BeautifulSoup(response, 'lxml')
    all_con = soup.find('dl', class_='clearfix zIndex5').find_all('div', class_='con')
    for con in all_con:
        all_a = con.find_all('a')
        for a in all_a[1:]:
            a_url = "http:" + a['href']
            location = a.get_text()
            rds.hset('ziru_hash', location, a_url)
            rds.rpush('ziru_name', location)

# 存入mongodb数据库
def save_to_mongo(total, client):
    db = client["ffzs"]
    collection = db[MONGO_DB]
    collection.insert_one(total)

# 获取图片上的数字
def pic2code(url):
    pic_name = 'price1.png'
    request.urlretrieve(url, pic_name)
    img = Image.open(pic_name)
    im = img.convert("L")
    im_new = Image.new(im.mode, (340, 70), "black")   # 创建新的背景使图片有留白便于识别
    im_new.paste(im, (20, 20))
    code = pytesseract.image_to_string(im_new)   # 使用tesseract进行数字识别
    return code

def get_info(key, rds, client):
    url = r.hget('ziru_hash', key)
    while True:
        try:
            resp = requests.get(url, headers=headers)
            if re.findall(r'var ROOM_PRICE = {"image":"(.*?)","offset":(.*?)};', resp.text):
                m = re.findall(r'var ROOM_PRICE = {"image":"(.*?)","offset":(.*?)};', resp.text)
                price_url = "http:"+m[0][0]
                # 判断价格图片是否已经有识别过了，识别过了的直接使用即可
                if r.hget('ziru_price', price_url):
                    code = r.hget('ziru_price', price_url)
                else:
                    code = pic2code(price_url)
                    rds.hset('ziru_price', price_url, code)
                soup = BeautifulSoup(resp.text, 'lxml')
                price_list = eval(m[0][1])
                location = key
                houses = soup.find("ul", id='houseList').find_all('li', class_='clearfix')
                for i, house in enumerate(houses):
                    title = house.find('a', class_='t1').get_text().split("·")[-1]
                    title1, face = title.split('-')
                    community = title1[:-3]
                    house_url ="http:" + house.find('a', class_='t1')['href']
                    price = ''.join(list(code[j] for j in price_list[i]))
                    if_first = house.find('span', class_='green').get_text(strip=True)
                    all_p = house.find('div', class_='detail').find_all('p')
                    subway = all_p[1].get_text(strip=True)
                    distance_subway = subway.split('线')[-1][:-1]
                    area, floor, type = all_p[0].get_text(strip=True).split('|')
                    tags = house.find('p', class_='room_tags clearfix').get_text('|', strip=True)
                    house_info = [location, community, type, house_url, area, floor, face, price, if_first, tags, distance_subway]
                    total = {}
                    for k, info in enumerate(house_info):
                        total[house_head[k]] = info
                    if key in subway:
                        save_to_mongo(total, client)
                        print('成功存入mongo：{}'.format(total))


                time.sleep(random.choice([1, 1.1, 1.2, 1.5, 1.8]))
                if soup.find('div', class_='pages').find('a', class_='next'):
                    next_url = "http:" + soup.find('div', class_='pages').find('a', class_='next')['href']
                    url = next_url
                else : break
            else : break
        except Exception as e:
            print(e)
            rds.rpush('ziru_error', url)
            continue


if __name__ == '__main__':
    headers = {
        'Referer': 'http://www.ziroom.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
    }
    client = pymongo.MongoClient("mongodb://localhost:27017", connect=False)
    pool = redis.ConnectionPool.from_url('redis://:666666@localhost:6379', db=1, decode_responses=True)
    r = redis.Redis(connection_pool=pool)
    house_head = ['location', 'community', 'type', 'url', 'area', 'floor', 'face', 'price', 'if_first',
                  'tags', 'distance_subway']
    get_location(r)
    while True:
        key = r.lpop('ziru_name')
        if key:
            print(key)
            get_info(key, r, client)
        else: break


