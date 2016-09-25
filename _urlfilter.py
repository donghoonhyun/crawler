# encoding=utf-8
def check_skip_url(url):
    try:
        if url.count('/photoview/') > 0:
            return True
        elif url.count('.0898.net/') > 0:
            return True
        elif url.count('/images/') > 0:
            return True
        elif url.count('/finance.sina.com.cn/') > 0:
            return True
        elif url.count('/games.sina.com.cn/') > 0:
            return True
        elif url.count('/video.sina.com.cn/') > 0:
            return True
        elif url.count('/mil.news.sina.com.cn/') > 0:
            return True
        elif url.count('/slide.ent.sina.com.cn/') > 0:
            return True
        elif url.count('/photo/') > 0:
            return True
        elif url.count('/picture.youth.cn/') > 0:
            return True
        elif url.count('pic.gmw.cn/') > 0:
            return True
        elif url.count('photo.gmw.cn/') > 0:
            return True
        elif url.count('/ah.people.com.cn/') > 0:
            return True
        elif url.count('/keywords/') > 0:
            return True
        elif url.count('.house.163.com/') > 0:
            return True
        elif url.count('help.3g.163.com/') > 0:
            return True
        elif url.count('news.163.com/') > 0:
            return True
        elif url.count('play.163.com/') > 0:
            return True
        elif url.count('cjn.cn/new/') > 0:
            return True
        elif url.count('/list/2/') > 0:
            return True
        elif url.count('/index.') > 0:
            return True
        elif url.count('finance.qq.com/') > 0:
            return True
        elif url.count('sports.qq.com/') > 0:
            return True
        elif url.count('stock.qq.com/') > 0:
            return True
        elif url.count('enorth.com.cn/') > 0:
            return True
        elif url.count('gxnews.com.cn/') > 0:
            return True
        elif url.count('qlwb.com.cn/') > 0:
            return True
        elif url.count('nen.com.cn/') > 0:
            return True
        elif url.count('pclady.com.cn/') > 0:
            return True
        elif url.count('rayli.com.cn/') > 0:
            return True
        elif url.count('chinadaily.com.cn/') > 0:
            return True
        elif url.count('/m/phone/') > 0:
            return True
        elif url.count('huanbohainews.com.cn/') > 0:
            return True
        elif url.count('ctocio.com.cn/') > 0:
            return True
        elif url.count('xkb.com.cn/') > 0:
            return True
        elif url.count('cngold.com.cn/') > 0:
            return True
        elif url.count('zol.com.cn/') > 0:
            return True
        elif url.count('qtv.com.cn/') > 0:
            return True
        elif url.count('gscn.com.cn/') > 0:
            return True
        elif url.count('hlbrdaily.com.cn/') > 0:
            return True
        elif url.count('/rollnews/') > 0:
            return True
        else:
            return False

    except Exception as e:
        print('check_skip_url : ' + str(e))  # a lot of log occurred
        return False


# for test
# test_url = 'http://ah.people.com.cn/BIG5/n2/2016/0913/c358329-28992343.html'
# print(test_url.count('/ah.people.com.cn/'))
# print(check_skip_url(test_url))
