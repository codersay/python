# -*- coding: utf-8 -*-

#!/usr/bin/env python
# -*- coding:utf-8 -*-

import threading

from Queue import Queue

from lxml import etree

import requests

import json,time,random

import urllib
import urllib2
import cookielib
import re

'''
本用例为公司项目所使用的案例的部分截取，实际功能比这块代码复杂
公开仅用于学习，禁止用于商业用途，后果自负
本用例爬取列表为 https://zixun.haodf.com/dispatched/20.htm?p=1
爬取的内容页为https://www.haodf.com/wenda/jijiyuanyuan_g_5143019232.htm
'''

class ThreadCrawl(threading.Thread):
    def __init__(self, threadName, pageQueue, dataQueue):
 
        super(ThreadCrawl, self).__init__() 
 
        self.threadName = threadName

        # 分页队列
        self.pageQueue = pageQueue

        # 队列
        self.dataQueue = dataQueue



        # 报头模拟
        self.headers = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv2.0.1) Gecko/20100101 Firefox/4.0.1",
            "Mozilla/5.0 (Windows NT 6.1; rv2.0.1) Gecko/20100101 Firefox/4.0.1",
            "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
            "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11"
        ]

    def run(self):
        print "启动 " + self.threadName
        while not CRAWL_EXIT:
            try:
                
                page = self.pageQueue.get(False)
                url = 'https://zixun.haodf.com/dispatched/20.htm?p=' + str(page)
                print 'list:' + url
                print ''

                response = requests.get(url, headers={"User-Agent": random.choice(self.headers)})
                resHtml = response.text
                text = etree.HTML(resHtml)

                node_list = text.xpath('//span[contains(@class, "fl")]')
                items = {}
                for node in node_list:
                    # xpath返回的列表，这个列表就这一个参数，用索引方式取出来，用户名
                    urli = node.xpath('./a[2]/@href')
                    if urli:
                        for u in urli:
                            self.dataQueue.put(u)

            except:
                pass
        print "列表页结束 " + self.threadName

class ThreadParse(threading.Thread):
    def __init__(self, threadName, dataQueue, filename, lock):
        super(ThreadParse, self).__init__() 
        self.threadName = threadName
        # 数据队列
        self.dataQueue = dataQueue
        #  解析后文件名
        self.filename = filename
        # lock
        self.lock = lock

        # 请求报头
        self.headers = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv2.0.1) Gecko/20100101 Firefox/4.0.1",
            "Mozilla/5.0 (Windows NT 6.1; rv2.0.1) Gecko/20100101 Firefox/4.0.1",
            "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
            "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11"
        ]

    def run(self):
        print "启动" + self.threadName
        while not PARSE_EXIT:
            try:
                url = self.dataQueue.get(False)
                self.parse(url)
            except:
                pass
        print "退出" + self.threadName

    def parse(self, url):
        spiderurl = url
        if url[0:5] != 'http:' or url[0:6] != 'https:':
            spiderurl = 'https:' + url
        print spiderurl
        response = requests.get(spiderurl, headers={"User-Agent": random.choice(self.headers)})
        resHtml = response.text
        # print(resHtml)
        text = etree.HTML(resHtml)
        # titles = text.xpath('//div[contains(@class,"bread-crumb-spacial")]/a')
        titles = text.xpath('//span[contains(@class, "fl")]/h1')
        title = titles[0].text  # 标题
        print title

        pagelists = text.xpath('//div[contains(@class,"zzx_yh_stream")]');
        pagecount = len(pagelists)

        pattern = re.compile("<a class=\"page_turn_a\" rel=\"true\">(.*?)</a>")
        m = pattern.search(resHtml)
        if m:
            pagetitle = m.group(1).encode('utf-8')
            if pagetitle:
                pagetitle = pagetitle.replace('共', '')
                pagetitle = pagetitle.replace('页', '')
                pagetitle = pagetitle.replace('&nbsp;', '')
                totalPage = int(pagetitle)
                if (totalPage - 1) > 0:
                    precount = (totalPage - 1) * 20  # 取总页之前的所有记录，比如 一共7页，取前6页的记录，最后一页的记录，需要爬取
                    end = spiderurl[-4:]
                    front = spiderurl[0:-4]
                    lastPageUrl = front + '_p_' + str(totalPage) + end

                    response = requests.get(lastPageUrl, headers={"User-Agent": random.choice(self.headers)})
                    resHtml = response.text
                    text = etree.HTML(resHtml)
                    pagelists = text.xpath('//div[contains(@class,"zzx_yh_stream")]');
                    lastPageCount = len(pagelists)
                    pagecount = int(lastPageCount + precount)
        print pagecount

        items = {
            "title" : title,
            "answeres_count" : pagecount,
            "url" : url,
        }

        print items

        time.sleep(5 * random.random())
        # title = title.replace(',', '，')
        # print title
 
        with self.lock:
            # 写入存储的解析后的数据
            # self.filename.write(json.dumps(items, ensure_ascii = False).encode("utf-8") + "\n")
            with open("haodf.csv", "a+") as f:
                f.write(title.encode('gbk').replace(",","，") + "," + str(pagecount) + "," + spiderurl + "\n")


CRAWL_EXIT = False
PARSE_EXIT = False


def main():
    # 页码队列 
    pageQueue = Queue(34) 
    for i in range(1, 35):
        pageQueue.put(i)

    # 采集结果(每页的HTML源码)的数据队列，参数为空表示不限制
    dataQueue = Queue()

    filename = open("好大夫.csv", "a+")
     
    lock = threading.Lock()
 
    crawlList = ["spierNo1", "spierNo2", "spierNo3"]
 
    threadcrawl = []
    for threadName in crawlList:
        thread = ThreadCrawl(threadName, pageQueue, dataQueue)
        thread.start()
        threadcrawl.append(thread)

 
    parseList = ["verifyNo1","verifyNo2","verifyNo3"]
  
    threadparse = []
    for threadName in parseList:
        thread = ThreadParse(threadName, dataQueue, filename, lock)
        thread.start()
        threadparse.append(thread)

    
    while not pageQueue.empty():
        pass

    # 如果pageQueue为空 退出循环
    global CRAWL_EXIT
    CRAWL_EXIT = True

    print "pageQueue为空"

    for thread in threadcrawl:
        thread.join()
        print "1"

    while not dataQueue.empty():
        pass

    global PARSE_EXIT
    PARSE_EXIT = True

    for thread in threadparse:
        thread.join()
        print "2"

    with lock:
        # 关闭文件
        filename.close()
    print "byebye！"

if __name__ == "__main__":
    main()