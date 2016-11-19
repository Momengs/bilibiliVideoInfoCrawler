#!usr/bin/python3
# -*- coding:utf-8 -*-
import json
import requests
import time
import pymongo
import logging
from multiprocessing.dummy import Pool as ThreadPool

connection = pymongo.MongoClient()
tdb = connection.text
postInfo = tdb.bilibiliVideo
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log.log',
                filemode='w+')
pool = ThreadPool(8)

def process(i):
    getStat = requests.get('http://api.bilibili.com/archive_stat/stat?aid='+str(i)).text
    stat = json.loads(getStat)['data']
    favorite = stat['favorite']
    coin = stat['coin']
    reply = stat['reply']
    view = stat['view']
    share = stat['share']
    danmaku = stat['danmaku']
    # getShow = requests.get('http://api.bilibili.com/x/elec/show?&aid='+str(i)).text
    # show4th = json.loads(getShow)['data']['list']  # 4 dicts in this list
    # showCount = json.loads(getShow)['data']['count']
    getTag = requests.get('http://api.bilibili.com/x/tag/archive/tags?aid='+str(i)).text
    try:
        tags = json.loads(getTag)['data']  # lots of dicts in this list
        totalTag = ''
        for tag in tags:
            tagName = tag['tag_name']
            totalTag += tagName+" "
        print(totalTag)
        getReplyCount = requests.get('http://api.bilibili.com/x/v2/reply?jsonp=jsonp&type=1&oid='+str(i)).text
        replyCount = json.loads(getReplyCount)['data']['page']['count']
        for n in range(1, 1+replyCount//20):
            getReply= requests.get('http://api.bilibili.com/x/v2/reply?jsonp=jsonp&type=1&sort=2&oid='+str(i)+'&pn='+str(n)).text
            replies = json.loads(getReply)['data']['replies']
            for content in replies:
                message = content['content']['message']
                ctime = content['ctime']
                like = content['like']
                uname = content['member']['uname']
                sex = content['member']['sex']
                mid = content['mid']
                item = {}
                item['AV号'] = i
                item['播放数'] = view
                item['弹幕数量'] = danmaku
                item['分享数量'] = share
                item['收藏数量'] = favorite
                item['硬币数量'] = coin
                item['视频标签'] = totalTag
                item['总评论数量'] = reply
                item['评论'] = message
                item['评论发表时间'] = time.ctime(ctime)
                item['评论点赞数量'] = like
                item['发布评论者'] = uname
                item['发布评论者ID'] = mid
                item['发布评论者性别'] = sex
                postInfo.insert(item)
    except Exception as e:
        logging.info('AV'+str(i)+"错误"+e)
        pass

result = pool.map(process, list(range(1,9999999)))
pool.close()
pool.join()

