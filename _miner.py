# encoding=utf-8
import urllib.request
from bs4 import BeautifulSoup
from _connectdb import *
from _domain import *
from _general import *
import logging
import datetime


class Miner:

    project_name = ''
    user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
    headers = {'User-Agent': user_agent, }
    timeout = 15

    def __init__(self, project_name, url):
        Miner.project_name = project_name
        Miner.url = url

        self.boot()
        self.miner_page('First_Miner', Miner.url)
        self.finish()

    @staticmethod
    def boot():
        logging.warning('[' + str(datetime.datetime.now()) + '] ' + 'Miner.boot()')

    # Finishes the spider
    @staticmethod
    def finish():
        logging.warning('[' + str(datetime.datetime.now()) + '] ' + 'Miner.finish()')

    # Updates user display, fills queue and updates files
    @staticmethod
    def miner_page(thread_name, page_url):
        logging.warning('[' + str(datetime.datetime.now()) + '] ' + thread_name + ' now crawling ' + page_url)
        logging.warning('[' + str(datetime.datetime.now()) + '] ' + 'TargetID : ' + Miner.project_name)

        domain = get_domain_name(page_url)

        if domain == '163.com':
            Miner.gather_163_gb2312_text(domain, page_url)
        elif domain == '365jia.cn':
            Miner.gather_365jia_text(domain, page_url)
        elif domain == '7y7.com':
            Miner.gather_7y7_text(domain, page_url)
        elif domain == 'aihami.com':
            Miner.gather_aihami_text(domain, page_url)
        elif domain == 'ce.cn':
            Miner.gather_ce_text(domain, page_url)
        elif domain == 'chinaxiaokang.com':
            Miner.gather_chinaxiaokang_text(domain, page_url)
        elif domain == 'cjn.cn':
            Miner.gather_cjn_text(domain, page_url)
        elif domain == 'cnr.cn':
            Miner.gather_cnr_general_text(domain, page_url)
        elif domain == 'cri.cn':
            Miner.gather_cri_text(domain, page_url)
        elif domain == 'dzwww.com':
            subdomain = get_sub_domain_name(page_url)
            if subdomain in ['weihai.dzwww.com', 'yantai.dzwww.com', 'laiwu.dzwww.com', 'qingdao.dzwww.com']:
                Miner.gather_dzwww_general_text(domain, page_url)
            elif subdomain in ['heze.dzwww.com', 'jinan.dzwww.com', 'liaocheng.dzwww.com', 'www.dzwww.com',
                               'zaozhuang.dzwww.com', 'dezhou.dzwww.com', 'www.dzwww.com']:
                Miner.gather_dzwww_heze_type_text(domain, page_url)
            elif subdomain in ['jining.dzwww.com']:
                Miner.gather_dzwww_heze_type_gb2312_text(domain, page_url)
        elif domain == 'gmw.cn':
            Miner.gather_gmw_text(domain, page_url)
        elif domain == 'hanyutai.com':
            Miner.gather_hanyutai_text(domain, page_url)
        elif domain == 'idol001.com':
            Miner.gather_idol001_text(domain, page_url)
        elif domain == 'ifeng.com':
            Miner.gather_ifeng_text(domain, page_url)
        elif domain == 'jsnol.com':
            Miner.gather_jsnol_text(domain, page_url)
        elif domain == 'juooo.com':
            Miner.gather_juooo_text(domain, page_url)
        elif domain == 'koreastardaily.com':
            Miner.gather_koreastardaily_text(domain, page_url)
        elif domain == 'kpopstarz.com':
            Miner.gather_kpopstarz_text(domain, page_url)
        elif domain == 'qianhuaweb.com':
            Miner.gather_qianhuaweb_text(domain, page_url)
        elif domain == 'qq.com':
            subdomain = get_sub_domain_name(page_url)
            if subdomain in ['ent.qq.com', 'fashion.qq.com']:
                Miner.gather_qq_general_text(domain, page_url)
            elif subdomain in ['cul.qq.com', 'games.qq.com']:
                Miner.gather_qq_general_gb2312_text(domain, page_url)
        elif domain == 'reyou.cn':
            Miner.gather_reyou_text(domain, page_url)
        elif domain == 'shenchuang.com':
            Miner.gather_shenchuang_text(domain, page_url)
        elif domain == 'taihainet.com':
            Miner.gather_taihainet_text(domain, page_url)
        elif domain == 'youth.cn':
            Miner.gather_youth_general_text(domain, page_url)
        elif domain == 'yulefm.com':
            Miner.gather_yulefm_text(domain, page_url)
        elif domain == 'zjrxz.com':
            Miner.gather_zjrxz_text(domain, page_url)
        elif domain == 'com.cn':
            subdomain = get_sub_domain_name(page_url)
            subsubdomain = get_sub_sub_domain_name(page_url)
            domain_type = get_domain_type(page_url)
            if subdomain == 'china.com.cn':
                if subsubdomain in ['jiangsu.china.com.cn', 'www.china.com.cn']:
                    Miner.gather_china_general_text(subdomain, page_url)
                elif subsubdomain in ['sd.china.com.cn']:
                    Miner.gather_china_sd_type_text(subdomain, page_url)
                else:
                    pass
            elif subdomain == 'people.com.cn':
                if subsubdomain in ['ent.people.com.cn', 'hb.people.com.cn', 'henan.people.com.cn', 'hlj.people.com.cn',
                                    'hn.people.com.cn', 'jx.people.com.cn', 'sc.people.com.cn', 'sz.people.com.cn',
                                    'sn.people.com.cn', 'ah.people.com.cn']:
                    Miner.gather_people_general_text(subdomain, page_url)
                elif subsubdomain == 'bj.people.com.cn':
                    if domain_type in ['GB', 'BIG5']:
                        Miner.gather_people_general_text(subdomain, page_url)
                    elif domain_type == 'n2':
                        Miner.gather_people_gb2312_text(subdomain, page_url)
                elif subsubdomain == 'cq.people.com.cn':
                    if domain_type == 'GB':
                        Miner.gather_people_cq_gb_text(subdomain, page_url)
                    elif domain_type == 'BIG5':
                        Miner.gather_people_general_text(subdomain, page_url)
                    elif domain_type == 'n2':
                        Miner.gather_people_gb2312_text(subdomain, page_url)
                elif subsubdomain == 'fj.people.com.cn':
                    if domain_type in ['n2', 'GB']:
                        Miner.gather_people_fj_gb2312_text(subdomain, page_url)
                    elif domain_type == 'BIG5':
                        Miner.gather_people_fj_text(subdomain, page_url)
                elif subsubdomain == 'gx.people.com.cn':
                    Miner.gather_people_gx_text(subdomain, page_url)
                elif subsubdomain == 'japan.people.com.cn':
                    Miner.gather_people_japan_text(subdomain, page_url)
                elif subsubdomain == 'korea.people.com.cn':
                    Miner.gather_people_korea_text(subdomain, page_url)
                elif subsubdomain == 'lady.people.com.cn':
                    if domain_type == 'BIG5':
                        Miner.gather_people_general_text(subdomain, page_url)
                    elif domain_type == 'n1':
                        Miner.gather_people_gb2312_text(subdomain, page_url)
                elif subsubdomain == 'sd.people.com.cn':
                    if domain_type == 'n2':
                        Miner.gather_people_gb2312_text(subdomain, page_url)
                    elif domain_type == 'BIG5':
                        Miner.gather_people_general_text(subdomain, page_url)
                else:
                    pass
            elif subdomain == 'sina.com.cn':
                if subsubdomain in ['ent.sina.com.cn', 'dj.sina.com.cn']:
                    Miner.gather_sina_general_text(subdomain, page_url)
                elif subsubdomain in ['news.sina.com.cn']:
                    Miner.gather_sina_news_type_text(subdomain, page_url)
                elif subsubdomain in ['sports.sina.com.cn']:
                    Miner.gather_sina_sports_type_text(subdomain, page_url)
                elif subsubdomain in ['hunan.sina.com.cn']:
                    Miner.gather_sina_hunan_type_text(subdomain, page_url)
                else:
                    pass

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_idol001_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')

            title = soup.find('h1', class_='news-title').text

            articles = soup.find('div', class_='article-detail').find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('div', class_='news-info clearfix').find_all('span')
            n = 1
            for i in infos:
                word = i.text
                if n == 1:
                    date = word
                elif n == 2:
                    if len(infos) == 2:
                        writer = ''
                        cnt = word[:-3]
                    else:
                        writer = word[3:]
                else:
                    cnt = word[:-3]
                n += 1

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_idol001_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_163_general_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text
            print(title)
            articles = soup.find('div', class_='article-detail').strings
            infos = soup.find('div', class_='news-info clearfix').findAll('span')

            article = ''
            for a_str in articles:
                article += a_str
            n = 1
            for i in infos:
                word = i.text
                if n == 1:
                    date = word
                elif n == 2:
                    if len(infos) == 2:
                        writer = ''
                        cnt = word[:-3]
                    else:
                        writer = word[3:]
                else:
                    cnt = word[:-3]
                n += 1

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print(str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_163_gb2312_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            decoded_html = plain_text.decode('gbk')
            soup = BeautifulSoup(decoded_html, 'html.parser')
            title = soup.find('h1').text
            articles = soup.find('div', class_='post_text').find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.replace('\n', ' ').strip()

            infos = soup.find('div', class_='post_time_source')
            date = infos.text[infos.text.index('-')-4:infos.text.index('-')+15]
            writer = soup.find('span', class_='ep-editor').text[5:].replace('"', '')
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print(str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_365jia_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h2', class_='nt_title').text
            article = soup.find('div', class_='nt_cont_txt').find('div', class_='').text.replace('\n', ' ')
            date = soup.find('span', class_='mr10').text + ' ' + soup.find('span', class_='mr15').text
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_365jia_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_taihainet_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1', class_='topic').text
            articles = soup.find('div', class_='article-content fontSizeSmall BSHARE_POP').find_all('p')

            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            date = soup.find('div', class_='sorce').find('span').text
            writer = soup.find('ul', class_='contentfooter').find('li', class_='r').find('span').text
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'find_all'":
                print('execute : gather_taihainet_2_text')
                Miner.gather_taihainet_2_text(fromsite, page_url)
            else:
                print('gather_taihainet_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_taihainet_2_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1', class_='topic').text
            articles = soup.find('div', class_='contnet_info').find_all('p')

            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('div', class_='page-info').find_all('span')
            date = infos[1].text
            writer = soup.find('span', {'id': 'editor_baidu'}).text
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_taihainet_2_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_qq_general_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')

            title = soup.find('h1').text

            articles = soup.find('div', {'class': 'qq_article'}).find_all('p', {'style': 'TEXT-INDENT: 2em'})
            article = ''
            for a_str in articles:
                article += a_str.text.strip()
            article = article[:article.index('var ')]

            date = soup.find('span', class_='a_time').text
            writer = soup.find('div', class_='qq_editor').text[5:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'text'":
                print('execute : gather_qq_general_2_text')
                Miner.gather_qq_general_2_text(fromsite, page_url)
            elif str(e) == "'NoneType' object has no attribute 'find'":
                print('execute : gather_qq_general_3_text')
                Miner.gather_qq_general_3_text(fromsite, page_url)
            elif str(e) == "substring not found":
                print('execute : gather_qq_general_4_text')
                Miner.gather_qq_general_4_text(fromsite, page_url)
            else:
                print('gather_qq_general_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_qq_general_2_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')

            title = soup.find('h1').text

            articles = soup.find('div', {'class': 'qq_article'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            date = soup.find('span', class_='a_time').text
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'text'":
                print('execute : gather_qq_general_5_text')
                Miner.gather_qq_general_5_text(fromsite, page_url)
            else:
                print('gather_qq_general_2_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_qq_general_3_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')

            title = soup.find('div', {'class': 'hd'}).find('h1').text

            articles = soup.find_all('p', {'style': 'TEXT-INDENT: 2em'})
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            date = soup.find('span', class_='a_time').text
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_qq_general_3_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_qq_general_4_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')

            title = soup.find('div', {'class': 'hd'}).find('h1').text

            articles = soup.find_all('p', {'style': 'TEXT-INDENT: 2em'})
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            date = soup.find('span', class_='a_time').text
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_qq_general_4_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_qq_general_5_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html5lib')

            title = soup.find('h1')

            print(title)  # TODO
            # articles = soup.find_all('p', {'style': 'TEXT-INDENT: 2em'})
            # article = ''
            # for a_str in articles:
            #     article += a_str.text.strip()
            #
            # date = soup.find('span', class_='a_time').text
            # writer = ''
            # cnt = '0'
            #
            # Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_qq_general_5_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_qq_general_gb2312_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            decoded_html = plain_text.decode('gbk')
            soup = BeautifulSoup(decoded_html, 'html.parser')

            title = soup.find('h1').text

            articles = soup.find('div', {'id': 'Cnt-Main-Article-QQ'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            date = soup.find('span', class_='article-time').text
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_qq_general_gb2312_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_kpopstarz_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text
            articles = soup.find('div', class_='at_body unselectable').findAll('p')

            article = ''
            for a_str in articles:
                if a_str.string == None:
                    pass
                else:
                    article += a_str.text.strip()

            infos = soup.find('p', class_='at_date').text
            date = infos[-16:].replace('年', '-').replace('月', '-').replace('日', ' ')
            cnt = '0'
            writer = infos[-25:-21]

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'findAll'":
                print('execute : gather_kpopstarz_2_text')
                Miner.gather_kpopstarz_2_text(fromsite, page_url)
            else:
                print('gather_kpopstarz_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_kpopstarz_2_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()
            article = article[article.index('|')+22:article.index('KpopStarz. All rights reserved')-7]

            infos = soup.find('p', class_='at_date').text.strip()
            date = infos[-16:].replace('年', '-').replace('月', '-').replace('日', ' ')
            cnt = '0'
            writer = infos[3:infos.index('|')-1].strip()

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_kpopstarz_2_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_people_general_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text
            articles = soup.find('div', class_='box_con').strings

            article = ''
            for a_str in articles:
                article += a_str.replace('\n', ' ').strip()
            infos = soup.find('div', class_='box01').find('div', class_='fl')
            date = infos.text[:17].replace('年', '-').replace('月', '-').replace('日', ' ')
            cnt = '0'

            writer = soup.find('div', class_='edit clearfix').text
            writer = writer.replace('(', '').replace(')', '')[3:]

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_people_general_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_people_cq_gb_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('div', class_='m_title').text
            articles = soup.find('div', class_='m_text').strings

            article = ''
            for a_str in articles:
                article += a_str.replace('\n', ' ').strip()

            infos = soup.find('p', class_='sjly').text
            date = infos[:20]
            cnt = '0'
            writer = infos[infos.index('责编：')+3:infos.index(')')]

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print(str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_people_gb2312_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            decoded_html = plain_text.decode('gbk')
            soup = BeautifulSoup(decoded_html, 'html.parser')

            title = soup.find('h1').text
            articles = soup.find('div', class_='box_con').strings

            article = ''
            for a_str in articles:
                article += a_str.replace('\n', ' ').strip()

            infos = soup.find('div', class_='box01').find('div', class_='fl').text
            date = infos[:17].replace('年', '-').replace('月', '-').replace('日', ' ')
            cnt = '0'

            writer = soup.find('div', class_='edit clearfix').text
            writer = writer.replace('(', '').replace(')', '')[3:]

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print(str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_people_gx_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text
            articles = soup.find('div', class_='content clear clearfix')
            articles = articles.find_all('p')

            article = ''
            for a_str in articles:
                article += a_str.text.replace('\n', ' ').strip()

            infos = soup.find_all('div', class_='page_c')
            date = infos[1].text[-21:].replace('年', '-').replace('月', '-').replace('日', ' ')
            cnt = '0'

            writer = soup.find('div', class_='edit clearfix').find('i').text
            writer = writer.replace('(', '').replace(')', '')[3:]

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print(str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_people_korea_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text
            articles = soup.find('div', class_='wb_p clear').strings

            article = ''
            for a_str in articles:
                article += a_str.replace('\n', ' ').strip()

            infos = soup.find('div', class_='w980 p1_content wb_content oh clear').find('h5')
            date = infos.text[:17].replace('年', '-').replace('月', '-').replace('日', ' ')
            cnt = '0'

            writer = soup.find('div', class_='editor').text
            writer = writer.replace('(', '').replace(')', '')[3:]

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print(str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_people_japan_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text
            article = soup.find('table', class_='summary').text
            article += soup.find('div', class_='show_text').text.replace('\n', ' ')

            infos = soup.find('p', class_='sou').text
            date = infos[:17].replace('年', '-').replace('月', '-').replace('日', ' ')
            cnt = '0'

            writer = soup.find('li', class_='fr').text
            writer = writer.replace('(', '').replace(')', '')[3:]

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print(str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_people_fj_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')

            title = soup.find('h1').text
            articles = soup.find('div', class_='text').find('div', class_='clearfix').findAll('p')

            article = ''
            for a_str in articles:
                article += a_str.text.replace('\n', ' ')

            infos = soup.find('div', class_='content_bg w1000_320 clearfix').find('h5').text
            date = infos[:17].replace('年', '-').replace('月', '-').replace('日', ' ')
            cnt = '0'

            writer = soup.find('div', class_='edit clearfix').find('i').text
            writer = writer.replace('(', '').replace(')', '')[3:]

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'find'":
                print('execute : gather_people_fj_2_text')
                Miner.gather_people_fj_2_text(fromsite, page_url)
            else:
                print('gather_people_fj_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_people_fj_2_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')

            title = soup.find('h1').text

            articles = soup.find('div', class_='fl text_con_left').find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('div', class_='box01').find('div', class_='fl').text[:16]
            date = infos.replace('年', '-').replace('月', '-').replace('日', ' ')
            writer = soup.find('div', class_='edit clearfix').text[4:-1]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_people_fj_2_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_people_fj_gb2312_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            decoded_html = plain_text.decode('gbk')
            soup = BeautifulSoup(decoded_html, 'html.parser')

            title = soup.find('h1').text
            articles = soup.find('div', class_='text').find('div', class_='clearfix').findAll('p')

            article = ''
            for a_str in articles:
                article += a_str.text.replace('\n', ' ')

            infos = soup.find('div', class_='content_bg w1000_320 clearfix').find('h5').text
            date = infos[:17].replace('年', '-').replace('月', '-').replace('日', ' ')
            cnt = '0'

            writer = soup.find('div', class_='edit clearfix').find('i').text
            writer = writer.replace('(', '').replace(')', '')[3:]

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'find'":
                print('execute : gather_people_fj_gb2312_2_text')
                Miner.gather_people_fj_gb2312_2_text(fromsite, page_url)
            else:
                print('gather_people_fj_gb2312_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_people_fj_gb2312_2_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            decoded_html = plain_text.decode('gbk')
            soup = BeautifulSoup(decoded_html, 'html.parser')

            title = soup.find('h1').text

            articles = soup.find('div', class_='box_con').find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.replace('\n', ' ').strip()

            infos = soup.find('div', class_='box01').find('div', class_='fl').text[:16]
            date = infos.replace('年', '-').replace('月', '-').replace('日', ' ')
            writer = soup.find('div', class_='edit clearfix').text[4:-1]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_people_fj_gb2312_2_text :' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_china_general_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text
            articles = soup.find('div', class_='content').find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('div', class_='col-left')
            date = infos.text[infos.text.index('发布时间：')+5:infos.text.index('发布时间：')+24]
            cnt = '0'
            writer = ''

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'find_all'":
                print('execute : gather_china_general_2_text')
                Miner.gather_china_general_2_text(fromsite, page_url)
            elif str(e) == "'NoneType' object has no attribute 'string'":
                print('execute : gather_china_general_3_text')
                Miner.gather_china_general_3_text(fromsite, page_url)
            elif str(e) == "'NoneType' object has no attribute 'text'":
                print('execute : gather_china_general_5_text')
                Miner.gather_china_general_5_text(fromsite, page_url)
            else:
                print('gather_china_general_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_china_general_2_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text
            articles = soup.find('div', {'id': 'artbody'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            date = soup.find('span', {'id': 'pubtime_baidu'}).text[5:]
            cnt = '0'
            writer = soup.find('span', {'id': 'editor_baidu'}).text[5:]

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'find_all'":
                print('execute : gather_china_general_4_text')
                Miner.gather_china_general_4_text(fromsite, page_url)
            else:
                print('gather_china_general_2_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_china_general_3_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('td', {'class': 'a4'}).text

            articles = soup.find('div', {'id': 'cc'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            date = soup.find('span', {'id': 'pubtime_baidu'}).text[5:]
            cnt = '0'
            writer = soup.find('span', {'id': 'author_baidu'}).text[4:]

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_china_general_3_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_china_general_4_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find('div', {'id': 'artiContent'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            date = soup.find('span', {'id': 'pubtime_baidu'}).text[5:]
            cnt = '0'
            writer = soup.find('span', {'id': 'author_baidu'}).text[4:]

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_china_general_4_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_china_general_5_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text.strip()

            articles = soup.find('div', {'id': 'artbody'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('div', {'class': 'info'}).find_all('small')
            date = infos[0].text
            writer = infos[3].text
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_china_general_5_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_china_sd_type_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find('div', {'id': 'mainCon'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            date = soup.find('div', class_='info').find('span', {'id': 'pubtime_baidu'}).text
            cnt = '0'
            writer = soup.find('div', class_='info').find('span', {'id': 'editor_baidu'}).text[5:]

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_china_sd_type_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_koreastardaily_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text
            articles = soup.find('div', {'id': 'content-body'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('div', {'id': 'content-title'}).find('h2').text
            date = infos[:11].replace('年', '-').replace('月', '-').replace('日', ' ').replace(' ', '') + ' '
            date += infos[infos.index('星期')+3:infos.index('星期')+8]
            writer = soup.find('div', {'id': 'content-title'}).find('h2').find('a').text
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_koreastardaily_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_reyou_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('div', class_='model_a').find('h2').text

            articles = soup.find_all('p')
            article = ''
            i = 1
            for a_str in articles:
                if i < len(articles)-1:
                    article += a_str.text.strip()
                i += 1

            infos = soup.find('div', class_='t_fu').find_all('label')
            date = infos[0].text.replace('.', '-')
            writer = infos[1].text
            cnt = soup.find('div', class_='share mt10 clear').find('label').text

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_koreastardaily_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_qianhuaweb_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1', class_='article-title').text

            articles = soup.find('div', class_='article-content fontSizeSmall BSHARE_POP').find_all('p')
            article = soup.find('p', class_='describe').text.replace('\n', ' ')[5:]
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('div', class_='article-infos').find('span', class_='date').text
            date = infos
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_qianhuaweb_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_7y7_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find('div', class_='art-body').find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()
            article = article.replace('to();', '')

            infos = soup.find('div', class_='art-auther').find_all('span')
            date = infos[0].text
            writer = infos[1].text[3:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_7y7_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_ifeng_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find('div', {'id': 'main_content'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('span', class_='ss01').text
            date = infos.replace('年', '-').replace('月', '-').replace('日', '')
            writer = soup.find('p', class_='iphone_none').text.replace('[', '').replace(']', '')[5:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'find_all'":
                print('execute : gather_ifeng_2_text')
                Miner.gather_ifeng_2_text(fromsite, page_url)
            else:
                print('gather_ifeng_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_ifeng_2_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h3').text

            articles = soup.find('div', {'class': 'arl-c-txt'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('span', {'id': 'pubtime_baidu'}).text
            date = infos.replace('年', '-').replace('月', '-').replace('日', '')
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_ifeng_2_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_shenchuang_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find('div', class_='bd_Article').find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('div', class_='tit-bar20').text
            date = infos[infos.index('| ')+2:infos.index('| ')+18]
            writer = infos[infos.index('编辑：')+3:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_shenchuang_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_ce_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text

            writer = article[article.index('责任编辑')+5:].replace(')', '').replace('）', '').replace('：', '')
            article = article[:article.index('责任编辑')-1].replace('\n', ' ').strip()

            infos = soup.find('span', {'id': 'articleTime'}).text
            date = infos.replace('年', '-').replace('月', '-').replace('日', '')
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "substring not found":
                print('execute : gather_ce_gb2312_text')
                Miner.gather_ce_gb2312_text(fromsite, page_url)
            else:
                print('gather_ce_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_ce_gb2312_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            decoded_html = plain_text.decode('gbk')
            soup = BeautifulSoup(decoded_html, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find('div', class_='TRS_Editor').find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            writer = soup.find('p', class_='zebian').text[7:-1]
            infos = soup.find('span', {'id': 'articleTime'}).text
            date = infos.replace('年', '-').replace('月', '-').replace('日', '')
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'text'":
                print('execute : gather_ce_2_text')
                Miner.gather_ce_2_text(fromsite, page_url)
            else:
                print('gather_ce_gb2312_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_ce_2_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            decoded_html = plain_text.decode('gbk')
            soup = BeautifulSoup(decoded_html, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find('div', {'id': 'articleText'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            writer = article[article.index('责任编辑')+5:-1]
            infos = soup.find('span', {'id': 'articleTime'}).text
            date = infos.replace('年', '-').replace('月', '-').replace('日', '')
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_ce_2_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_sina_general_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find('div', class_='content').find_all('p')
            article = soup.find('p', class_='ellipsis').text
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('span', class_='titer').text
            date = infos.replace('年', '-').replace('月', '-').replace('日', '')
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_sina_general_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_sina_news_type_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find('div', class_='article article_16').find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('span', class_='time-source').text[:17]
            date = infos.replace('年', '-').replace('月', '-').replace('日', ' ')
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_sina_news_type_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_sina_sports_type_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1', {'id': 'artibodyTitle'}).text.strip()

            articles = soup.find('div', {'id': 'artibody'}).find_all(['p', 'span'])
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('span', {'id': 'pub_date'}).text.strip()
            date = infos.replace('年', '-').replace('月', '-').replace('日', ' ')
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_sina_sports_type_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_sina_hunan_type_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find('div', {'class': 'article-body main-body'}).find_all(['p', 'span'])
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('p', {'class': 'source-time'}).find('span').text
            date = infos
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_sina_hunan_type_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_youth_general_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('div', class_='l_tit').text

            articles = soup.find('div', {'id': 'articleText'}).find_all('p')
            article = ''
            n = 1
            for a_str in articles:
                if n in [1,  2]:
                    pass
                else:
                    article += a_str.text.strip()
                n += 1

            infos = soup.find('span', {'id': 'pubtime_baidu'}).text[5:]
            date = infos
            writer = soup.find('span', {'id': 'editor_baidu'}).text[5:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'find_all'":
                print('execute : gather_youth_general_2_text')
                Miner.gather_youth_general_2_text(fromsite, page_url)
            else:
                print('gather_youth_general_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_youth_general_2_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('div', class_='l_tit').text

            articles = soup.find('div', {'class': 'article'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('span', {'id': 'pubtime_baidu'}).text[5:]
            date = infos
            writer = soup.find('span', {'id': 'editor_baidu'}).text[5:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_youth_general_2_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_dzwww_general_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h2').text

            articles = soup.find('div', {'class': 'TRS_Editor'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('span', {'id': 'pubtime_baidu'}).text[5:]
            date = infos
            writer = soup.find('span', {'id': 'editor_baidu'}).text[5:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'text'":
                print('execute : gather_dzwww_general_2_text')
                Miner.gather_dzwww_general_2_text(fromsite, page_url)
            else:
                print('gather_dzwww_general_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_dzwww_general_2_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h2').text

            articles = soup.find('div', {'class': 'TRS_Editor'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('span', {'id': 'pubtime_baidu'}).text[5:]
            date = infos
            infos = soup.find('div', {'class': 'edit'}).text
            writer = infos[infos.index('责任编辑')+5:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'text'":
                print('execute : gather_dzwww_general_3_text')
                Miner.gather_dzwww_general_3_text(fromsite, page_url)
            elif str(e) == "substring not found":
                print('execute : gather_dzwww_heze_type_gb2312_text')
                Miner.gather_dzwww_heze_type_gb2312_text(fromsite, page_url)
            else:
                print('gather_dzwww_general_2_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_dzwww_general_3_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find('div', {'id': 'news-con'}).find('div', {'class': 'TRS_Editor'}).find_all('p')
            article = soup.find('div', {'id': 'news-header'}).find('div', {'class': 'summary'}).text.strip()
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('span', {'id': 'pubtime_baidu'}).text
            date = infos.replace('年', '-').replace('月', '-').replace('日', '')
            infos = soup.find('div', {'id': 'editor'}).text
            writer = infos[infos.index('责任编辑')+5:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_dzwww_general_3_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_dzwww_heze_type_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h2').text

            articles = soup.find('div', {'class': 'TRS_Editor'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('div', {'class': 'layout'}).find('div', {'class': 'left'}).text[:19]
            date = infos
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'find_all'":
                print('execute : gather_dzwww_heze_type_2_text')
                Miner.gather_dzwww_heze_type_2_text(fromsite, page_url)
            elif str(e) == "'NoneType' object has no attribute 'text'":
                print('execute : gather_dzwww_heze_type_gb2312_2_text')
                Miner.gather_dzwww_heze_type_gb2312_2_text(fromsite, page_url)
            else:
                print('gather_dzwww_heze_type_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_dzwww_heze_type_2_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h2').text

            articles = soup.find('div', {'class': 'news-con'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('div', {'class': 'layout'}).find('div', {'class': 'left'}).text[:19]
            date = infos
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_dzwww_heze_type_2_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_dzwww_heze_type_gb2312_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            decoded_html = plain_text.decode('gbk')
            soup = BeautifulSoup(decoded_html, 'html.parser')
            title = soup.find('h2').text

            articles = soup.find('div', {'class': 'TRS_Editor'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('div', {'class': 'layout'}).find('div', {'class': 'left'}).text[:19]
            date = infos
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'find'":
                print('execute : gather_dzwww_heze_type_gb2312_3_text')
                Miner.gather_dzwww_heze_type_gb2312_3_text(fromsite, page_url)
            else:
                print('gather_dzwww_heze_type_gb2312_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_dzwww_heze_type_gb2312_2_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            decoded_html = plain_text.decode('gbk')
            soup = BeautifulSoup(decoded_html, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find('div', {'id': 'news-con'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('div', {'class': 'infor'}).find('div', {'class': 'left'}).find('span').text
            date = infos
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_dzwww_heze_type_gb2312_2_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_dzwww_heze_type_gb2312_3_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            decoded_html = plain_text.decode('gbk')
            soup = BeautifulSoup(decoded_html, 'html.parser')
            title = soup.find('h2').text

            articles = soup.find('div', {'class': 'TRS_Editor'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('div', {'id': 'news-con'}).find_all('span')
            date = infos[2].text
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_dzwww_heze_type_gb2312_3_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_aihami_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find('div', {'id': 'text'}).find_all('p')
            article = ''
            n = 0
            for a_str in articles:
                if n in [len(articles)-1]:
                    pass
                else:
                    article += a_str.text.replace('\n', '').strip()
                n += 1

            infos = soup.find('div', {'id': 'text'}).find_all('div')
            date = infos[1].text[7:].replace('年', '-').replace('月', '-').replace('日', ' ')
            writer = soup.find('p', {'class': 'editorSign'}).find('span').text[5:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_aihami_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_juooo_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find('div', {'class': 'ns-info'}).find_all(['p', 'span'])
            article = ''
            for a_str in articles:
                article += a_str.text.replace('\n', '').strip()

            infos = soup.find('div', {'class': 'ntit-bar'}).text
            date = infos[3:19].replace('.', '-')
            writer = ''
            infos = infos[infos.index('浏览')+3:]
            cnt = infos[:infos.index('|')-2]

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_juooo_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_yulefm_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text

            articles = soup.find('div', {'id': 'content-body'}).find_all(['td'])

            article = ''
            for a_str in articles:
                article += a_str.text.replace('\n', '').strip()

            infos = soup.find('div', {'id': 'content-title'}).find('h2').text.strip()
            date = infos[:19].strip()
            writer = infos[26:].strip()
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_yulefm_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_cnr_general_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('title').text[:-4]

            articles = soup.find('div', {'class': 'TRS_Editor'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.replace('\n', '').strip()

            infos = soup.find('span', {'id': 'pubtime_baidu'}).text
            date = infos
            writer = soup.find('span', {'id': 'editor_baidu'}).text[3:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'text'":
                print('execute : gather_cnr_general_2_text')
                Miner.gather_cnr_general_2_text(fromsite, page_url)
            else:
                print('gather_cnr_general_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_cnr_general_2_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('title').text[:-4]

            articles = soup.find('div', {'class': 'TRS_Editor'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.replace('\n', '').strip()

            infos = soup.find('p', {'class': 'lh30 left f14 yahei'}).text
            date = infos[:19]
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_cnr_general_2_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_zjrxz_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('title').text[:-5]

            articles = soup.find('div', {'class': 'article-content fontSizeSmall'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.replace('\n', '').strip()

            infos = soup.find('span', {'class': 'date'}).text
            date = infos
            writer = soup.find('div', class_='editor').text[6:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'find_all'":
                print('execute : gather_zjrxz_2_text')
                Miner. gather_zjrxz_2_text(fromsite, page_url)
            else:
                print('gather_zjrxz_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_zjrxz_2_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('title').text[:-5]

            articles = soup.find('div', {'class': 'article-content fontSizeSmall BSHARE_POP'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.replace('\n', '').strip()

            infos = soup.find('span', {'class': 'date'}).text
            date = infos
            writer = soup.find('div', class_='editor').text[6:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_zjrxz_2_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_gmw_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1', {'id': 'articleTitle'}).text.strip()

            articles = soup.find('div', {'id': 'contentMain'}).text
            article = articles.replace('\n', '').strip()

            date = soup.find('span', {'id': 'pubTime'}).text
            writer = soup.find('div', {'id': 'contentLiability'}).text.replace('[', '').replace(']', '')[5:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_gmw_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_hanyutai_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h4', {'class': 'text-center'}).text.strip()

            articles = soup.find('div', {'class': 'wp-art-content'}).text
            article = articles.replace('\n', '').strip()

            infos = soup.find('p', {'class': 'text-center'}).find_all('span')
            date = infos[1].text[5:]
            writer = infos[0].text[3:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'text'":
                print('execute : gather_hanyutai_2_text')
                Miner.gather_hanyutai_2_text(fromsite, page_url)
            else:
                print('gather_hanyutai_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_hanyutai_2_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1', {'class': 'ph'}).text.strip()

            articles = soup.find('div', {'class': 't_fsz'}).find('td').text
            article = articles[articles.index('上传')+2:].strip()

            infos = soup.find('div', {'class': 'h hm'}).find('p', {'class': 'xg1'}).text
            date = infos[infos.index('发布时间')+5:infos.index('发布时间')+20].strip()
            writer = infos[infos.index('采编')+3:infos.index('|')].strip()
            cnt = infos[infos.index('查看数')+5:].strip()

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_hanyutai_2_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_cjn_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1', {'class': 'ph'}).text.strip()

            articles = soup.find('div', {'class': 'd d-detail'}).find_all('p')
            article = soup.find('div', {'class': 'd d-detail'}).text.replace('\n', '').strip()
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('div', {'class': 'h hm'}).find('p', {'class': 'xg1'}).text
            date = infos[:16]
            writer = ''
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_cjn_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_chinaxiaokang_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text.strip()

            articles = soup.find('div', {'id': 'content'}).find_all('p')
            article = soup.find('div', {'class': 'desc'}).text.replace('\n', '').strip()
            for a_str in articles:
                article += a_str.text.strip()

            infos = soup.find('div', {'class': 'time'}).find_all('span')
            date = infos[0].text
            writer = infos[2].text[3:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_chinaxiaokang_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_cri_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text.strip()

            articles = soup.find('div', {'id': 'abody'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            date = soup.find('span', {'id': 'acreatedtime'}).text
            writer = soup.find('div', {'class': 'editor'}).text[4:-1]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            if str(e) == "'NoneType' object has no attribute 'text'":
                print('execute : gather_cri_2_text')
                Miner.gather_cri_2_text(fromsite, page_url)
            else:
                print('gather_cri_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_cri_2_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text.strip()

            articles = soup.find('div', {'id': 'abody'}).find_all('p')
            article = ''
            for a_str in articles:
                article += a_str.text.strip()

            date = soup.find('span', {'id': 'acreatedtime'}).text
            writer = soup.find('span', {'id': 'aeditor'}).text[3:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_cri_2_text : ' + str(e))
            return set()
        return

    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_jsnol_text(fromsite, page_url):
        try:
            request = urllib.request.Request(page_url, None, Miner.headers)
            response = urllib.request.urlopen(request, timeout=Miner.timeout)

            plain_text = response.read()
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find('h1').text.strip()

            article = soup.find('div', {'class': 'content'}).text.strip()

            infos = soup.find('div', {'class': 'title'}).find_all('em')
            date = infos[0].text
            writer = infos[2].text[3:]
            cnt = '0'

            Miner.save_db(Miner.project_name, page_url, title, article, fromsite, date, writer, cnt)

        except Exception as e:
            print('gather_jsnol_text : ' + str(e))
            return set()
        return

    @staticmethod
    def save_db(targetid, url, title, article, fromsite, date, writer, cnt):
        test = 2
        if test == 1:
            print(targetid, url, title, article.strip(), fromsite, date, writer, cnt)
        else:
            save_article(targetid, url, title.replace("'","%20").strip(), article.replace("'", "%20").strip(),
                         fromsite.strip(), date_formatter(date), writer.strip(), cnt.strip())
            mark_url(targetid, url)

# for test
# Miner('88', 'http://fashion.sina.com.cn/b/nw/2016-09-13/1023/doc-ifxvukhx5004140.shtml')
