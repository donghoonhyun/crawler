# encoding=utf-8
from urllib.request import urlopen
from bs4 import BeautifulSoup
from urllib import parse
from _connectdb import *
from _domain import *
from _urlfilter import *
import logging
import datetime


class Collector:

    project_name = ''
    base_url = ''
    domain_name = ''
    queue = set()
    crawled = set()

    def __init__(self, project_name, base_url, domain_name):
        Collector.project_name = project_name
        Collector.base_url = base_url
        Collector.domain_name = domain_name
        self.boot()
        self.crawl_page('First_Collector', Collector.base_url)
        self.finish()

    # Creates directory and files for project on first run and starts the collector
    @staticmethod
    def boot():
        logging.warning('[' + str(datetime.datetime.now()) + '] ' + 'Collector.boot()')
        Collector.queue.add(Collector.base_url)

    # Finishes the spider
    @staticmethod
    def finish():
        logging.warning('[' + str(datetime.datetime.now()) + '] ' + 'Collector.finish()')

    # Updates user display, fills queue and updates files
    @staticmethod
    def crawl_page(thread_name, page_url):
        if page_url not in Collector.crawled:
            logging.warning('[' + str(datetime.datetime.now()) + '] ' + thread_name + ' now collecting ' + page_url)
            logging.warning('[' + str(datetime.datetime.now()) + '] ' + 'TargetID : ' + Collector.project_name)
            Collector.add_links_to_queue(Collector.gather_links(page_url))
            Collector.queue.remove(page_url)
            Collector.crawled.add(page_url)
            Collector.save_data()

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_links(page_url):
        try:
            response = urlopen(page_url)
            if 'text/html' in response.getheader('Content-Type'):
                plain_text = response.read()
                soup = BeautifulSoup(plain_text, 'html5lib')
            page_links = set()
            links = soup.find_all('a')
            for l in links:
                url = parse.urljoin(Collector.base_url, l.get('href'))
                if 'javascript' not in url:
                    page_links.add(url)

        except Exception as e:
            print(str(e))
            return set()
        return page_links

    # Saves queue data to project files
    @staticmethod
    def add_links_to_queue(links):
        for url in links:
            if (url in Collector.queue) or (url in Collector.crawled):
                continue
            if Collector.domain_name == get_domain_name(url):
                continue
            if check_skip_url(url):
                continue
            if check_in_white_list(url):
                Collector.queue.add(url)

    @staticmethod
    def save_data():
        save_url(Collector.project_name, Collector.queue)
        Collector.queue.clear()
        Collector.crawled.clear()

# for test
# Collector('1', 'http://news.baidu.com/ns?ct=0&rn=20&pn=0&ie=utf-8&rsv_bp=1&sr=0&cl=2&f=8&prevct=no&tn=news&word=exo',
#  'baidu.com')
# mark_target('1')
