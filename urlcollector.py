# encoding=utf-8
from _connectdb import *
from _collector import Collector
from _domain import *

init_target_list()
target_list = get_target_list()
for target in target_list:
    word = target[1]
    project_name = str(target[0])
    homepage = 'http://news.baidu.com/'

    domain_name = get_domain_name(homepage)

    for i in range(0, 3):
        homepage = 'http://news.baidu.com/ns?ct=0&rn=20&pn=' + str(i*20) + '&ie=utf-8&rsv_bp=1&'\
                   + 'sr=0&cl=2&f=8&prevct=no&tn=news&word=' + word
        Collector(project_name, homepage, domain_name)
    mark_target(project_name)
