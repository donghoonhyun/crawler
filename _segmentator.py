# encoding=utf-8
import jieba.analyse
import pandas
import re
from _connectdb import *


def segmenter(amode, pmode, doc_info, topk, stop_words):
    targetid = doc_info[0]
    url = doc_info[1]
    doc = doc_info[2]
    doc = preprocess(doc)

    if amode == 'F':
        seg_list = jieba.lcut(doc, cut_all=True)
        if pmode == 'P':
            print("Full Mode:")
            print("/ ".join(seg_list))
        elif pmode == 'D':
            print(seg_list)

    elif amode == 'D':
        seg_list = jieba.lcut(doc, cut_all=False)
        if pmode == 'P':
            print("Default Mode:")
            print("/ ".join(seg_list))
        elif pmode == 'D':
            process(amode, targetid, url, seg_list, stop_words)

    elif amode == 'S':
        seg_list = jieba.lcut_for_search(doc)
        if pmode == 'P':
            print("Search Mode:")
            print("/ ".join(seg_list))
        elif pmode == 'D':
            print(seg_list)

    elif amode == 'E':
        jieba.analyse.set_stop_words("./stop_words/stop_words_chinese.txt")
        seg_list = jieba.analyse.extract_tags(doc, topK=topk, withWeight=False)
        if pmode == 'P':
            print("Extract Mode:")
            print("/ ".join(seg_list))
            for x, w in jieba.analyse.textrank(doc, topK=topk, withWeight=True):
                print('%s %s' % (x, w))
        elif pmode == 'D':
            process(amode, targetid, url, seg_list, stop_words)

    elif amode == 'A':
        seg_list = jieba.cut(doc, cut_all=True)
        if pmode == 'P':
            print("Full Mode:")
            print("/ ".join(seg_list))
            print("")
        elif pmode == 'D':
            print(seg_list)

        seg_list = jieba.cut(doc, cut_all=False)
        if pmode == 'P':
            print("Default Mode:")
            print("/ ".join(seg_list))
            print("")
        elif pmode == 'D':
            print(seg_list)

        seg_list = jieba.cut_for_search(doc)
        if pmode == 'P':
            print("Search Mode:")
            print("/ ".join(seg_list))
            print("")
        elif pmode == 'D':
            print(seg_list)

        # jieba.analyse.set_stop_words("./extra_dict/stop_words.txt")
        seg_list = jieba.analyse.extract_tags(doc, topK=topk, withWeight=False, allowPOS=())
        if pmode == 'P':
            print("Extract Mode:")
            print("/ ".join(seg_list))
        elif pmode == 'D':
            print(seg_list)


def preprocess(doc):
    # remove blank
    doc = doc.replace('  ', ' ').replace('-', ' ').replace('–', ' ').replace('+', ' ').replace('*', ' ')
    doc = doc.replace('!', ' ').replace('{', ' ').replace('}', ' ').replace('￣', ' ')
    doc = doc.replace('(', ' ').replace(')', ' ').replace(',', ' ').replace('?', ' ')
    doc = doc.replace('Ｉ', ' ').replace('|', ' ').replace('Ｉ', ' ')
    doc = doc.replace('[', ' ').replace(']', ' ').replace('’', ' ').replace('“', ' ').replace('”', ' ')
    doc = doc.replace('\r', '').replace('\t', '').replace('\n', '').replace('[\b]', '').replace('~', ' ')
    # remove special symbol
    doc = re.sub('[：:;；/《》<>（）【】「」╰╯╭╮、，。°・·,.`´“”‘’"×？！^%~＝=…＆&#@\d\s]', ' ', doc)
    doc = re.sub('[♥❤♣♡☘☻☟☞☕☉⭐☆★◡◕●○◉◇▽▼▶△▲□■┻┑┍─⑨_Øʖ＾ˆˇ˘˙˵˶ΟΠΣΩ]', ' ', doc)
    doc = re.sub('[ДЗԄ،—↓↑←→ؤأءⅡ∀∇℃•※‿OÔ√①②③ㅠＡＢ😄😡😂⛳🐷💋💖😍😘😱❣🐱💙🙏]', ' ', doc)
    doc = re.sub('[🇨🇸🇺🍂🍃👇🔥👊👏🏻🏽🏾🏿🍎🐴😁àùúāăĭŏŭ💤😴🏆🐍🐜📸🎪📹🌄👍🕛👻😎🙊]', ' ', doc)
    doc = re.sub('[💕😆😏🙆🍩🍰🎃🎄👀👋👑💃🌳🌴🌵🍇🔪😅😖😗😲😉😊😽😿🌹🎁🎂🎉🌸🚴]', ' ', doc)
    doc = re.sub('[🍯🎈🌞💘💪🍌👉💧🕊]', ' ', doc)
    # change all english character to upper character
    doc = doc.lower()

    # jieba word suggest
    jieba.suggest_freq('EXO', True)
    jieba.suggest_freq('dancing-king', True)
    jieba.suggest_freq('running-man', True)
    jieba.suggest_freq('red-velvet', True)
    jieba.suggest_freq('TFBOYS', True)
    jieba.suggest_freq('无限挑战', True)
    jieba.suggest_freq('刘在石', True)
    jieba.suggest_freq('防弹少年团', True)
    jieba.suggest_freq('太阳的后裔', True)
    jieba.suggest_freq('李钟硕', True)
    jieba.suggest_freq('李敏镐', True)
    jieba.suggest_freq('朴宝剑', True)
    jieba.suggest_freq('云画的月光', True)
    jieba.suggest_freq('黄致列', True)
    jieba.suggest_freq('刘诗诗', True)
    jieba.suggest_freq('霍建华', True)
    jieba.suggest_freq('迪丽热巴', True)
    jieba.suggest_freq('林更新', True)
    jieba.suggest_freq('马天宇', True)
    jieba.suggest_freq('黄景瑜', True)
    jieba.suggest_freq('徐海乔', True)
    jieba.suggest_freq('华晨宇', True)
    jieba.suggest_freq('X玖少年团', True)

    return doc


def remove_stop_words(df, stop_words):
    tmp_list = []
    tmp_dict = {}
    for i in range(len(df)):
        if df['word'].loc[i] in stop_words:
            pass
        elif df['word'].loc[i] == ' ':
            pass
        elif len(df['word'].loc[i]) > 40:
            pass
        else:
            tmp_dict = {'word': df['word'].loc[i], 'count': df['count'].loc[i]}
            tmp_list.append(tmp_dict)
    df = pandas.DataFrame(tmp_list, columns=['word', 'count'])
    return df


def process(amode, targetid, url, seg_list, stop_words):
    frame = pandas.DataFrame(seg_list, columns=['word'])
    gframe = frame.groupby(['word']).size().reset_index(name='count')
    gframe = remove_stop_words(gframe, stop_words)
    gframe['targetID'] = targetid
    gframe['url'] = url
    save_db(amode, targetid, url, gframe)


def save_db(amode, targetid, url, df):
    if amode == 'D':
        save_word(targetid, url, df)
    elif amode == 'E':
        save_keyword(targetid, url, df)
