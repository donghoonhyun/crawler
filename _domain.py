from urllib.parse import urlparse


# Get domain name (example.com)
def get_domain_name(url):
    try:
        results = get_full_domain_name(url).split('.')
        return results[-2] + '.' + results[-1]

    except Exception as e:
        print('get_domain_name : ' + str(e))
        return ''


# Get domain name (example.com)
def get_sub_domain_name(url):
    try:
        results = get_full_domain_name(url).split('.')
        return results[-3] + '.' + results[-2] + '.' + results[-1]

    except Exception as e:
        print('get_sub_domain_name : ' + str(e))
        return ''


# Get sub domain name (name.example.com)
def get_sub_sub_domain_name(url):
    try:
        results = get_full_domain_name(url).split('.')
        return results[-4] + '.' + results[-3] + '.' + results[-2] + '.' + results[-1]

    except Exception as e:
        print('get_sub_sub_domain_name : ' + str(e))
        return ''


# Get sub domain name (name.example.com)
def get_full_domain_name(url):
    try:
        return urlparse(url).netloc

    except Exception as e:
        print('get_full_domain_name : ' + str(e))
        return ''


# Get sub domain name (name.example.com)
def get_domain_type(url):
    try:
        if url.count('people.com.cn/GB/') == 1:
            domain_type = 'GB'
        elif url.count('people.com.cn/BIG5/') == 1:
            domain_type = 'BIG5'
        elif url.count('people.com.cn/n1/') == 1:
            domain_type = 'n1'
        elif url.count('people.com.cn/n2/') == 1:
            domain_type = 'n2'
        elif url.count('people.com.cn/n3/') == 1:
            domain_type = 'n3'
        else:
            domain_type = ''

        return domain_type

    except Exception as e:
        print('get_domain_type : ' + str(e))
        return ''


# When a domain exists in the white list, return True
def check_in_white_list(url):
    try:
        domain = get_domain_name(url)
        if domain in ['163.com', '365jia.cn', '7y7.com', 'aihami.com', 'ce.cn', 'com.cn', 'chinaxiaokang.com', 'cjn.cn',
                      'cnr.cn', 'cri.cn', 'dzwww.com', 'gmw.cn', 'hanyutai.com', 'idol001.com', 'ifeng.com',
                      'juooo.com', 'koreastardaily.com', 'kpopstarz.com', 'qianhuaweb.com', 'qq.com', 'reyou.cn',
                      'shenchuang.com', 'taihainet.com', 'youth.cn', 'yulefm.com', 'zjrxz.com']:
            return True
        else:
            return False

    except Exception as e:
        print('check_in_white_list : ' + str(e))
        return False
