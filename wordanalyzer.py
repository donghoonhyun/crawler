from _segmentator import *
from _stopwords import *

amode = 'E'  # 'F': Full mode, 'D': Default mode, 'S': cut_for_search mode, 'E': Extract tags mode
pmode = 'D'  # 'P': Print mode, 'D': Database mode
topK = 50

doc_infos = get_document()
stop_words = get_stop_words('cn')
for doc_info in doc_infos:
    segmenter(amode, pmode, doc_info, topK, stop_words)
