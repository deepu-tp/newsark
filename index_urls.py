import requests
import lxml
import lxml.html
from urllib.parse import urljoin
import math
import pandas
import fs
from fs.osfs import OSFS
from datetime import datetime
import time
from multiprocessing import Pool
from datetime import date, timedelta
import hashlib
import sys

from elasticsearch_dsl import Document, Date, Integer, Keyword, Text, Index
from elasticsearch_dsl.connections import connections
from elasticsearch.helpers import bulk


connections.create_connection(hosts=['172.18.0.2'], timeout=20)

archives = Index('archives_v1')
# define custom settings
archives.settings(
    number_of_shards=1,
    number_of_replicas=0
)

@archives.document
class Article(Document):
    title = Text()
    link = Text()
    date = Date()
    source = Keyword()
    keywords = Keyword()

try:
    Article.init()
except:
    pass


client = connections.get_connection()

if __name__ == '__main__':

    _folder = sys.argv[1]

    t = {
        "toi": 1001094,
        "hindu": 3113377,
        "indian_express": 92956
    }

    ofs = OSFS("archives/%s" % _folder)
    tc = t[_folder]
    c = 0
    documents = []
    for f in ofs.walk.files(filter=["*manifest.csv"]):
        _p = ofs.getsyspath(f)
        folder = fs.path.combine(fs.path.dirname(_p), "_pages")
        _fs = OSFS(folder, create=True)
        try:
            _df = pandas.read_csv(_p)
        except pandas.errors.EmptyDataError:
            continue


        _df = _df.fillna("")
        for row in _df.to_dict(orient="records"):
            c += 1

            if not row['link'].startswith("http"):
                continue

            keywords = []
            if "section" in row:
                keywords.append(row['section'])

            art = Article(**{
                "meta": {
                    "id": hashlib.md5(row['link'].encode("ascii")).hexdigest()
                },
                "date": row['date'],
                "source": row['source'],
                "keywords": keywords,
                "title": row['text'],
                "link": row['link']               
            })
            documents.append(art)

            if len(documents) % 1000 == 0:
                print("Indexing... %s / %s" % (c , tc))
                docs = (d.to_dict(include_meta=True) for d in documents)
                bulk(client, docs)
                documents = []

    if len(documents):
        print("Indexing... %s / %s" % (c , tc))
        docs = (d.to_dict(include_meta=True) for d in documents)
        bulk(client, docs)
        documents = []
