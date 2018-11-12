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
from requests.exceptions import SSLError

def scrape_toi(config):
    sy, ey, sm, em, sd, ed = config
    ofs = OSFS("archives/toi", create=True)
    # Found in the JS on the page with the calendar on it.
    base_date = datetime(1899, 12, 30)
    for year in range(sy, ey + 1):
        yfs = ofs.makedir(str(year), recreate=True)
        for month in range(sm, em + 1):
            mfs = yfs.makedir(str(month), recreate=True)
            for day in range(sd, ed + 1):
                try:
                    cd = datetime(year, month, day)
                except ValueError:
                    continue

                start = math.floor((cd - base_date).total_seconds() / 86400)
                if start > 43300:
                    break

                dfs = mfs.makedir(str(day), recreate=True)

                _file = cd.strftime("%Y-%m-%d_manifest.csv")
                _path = dfs.getsyspath(_file)

                if dfs.exists(_file):
                    continue

                url = "https://timesofindia.indiatimes.com/%d/%d/%d/archivelist/year-%d,month-%d,starttime-%d.cms" % (year, month, day, year, month, start)
                print(url)
                resp = requests.get(url)
                doc = lxml.html.fromstring(resp.content)

                rows = []

                for el in doc.xpath('/html/body/div[1]/table[2]//tr[2]//a'):
                    link = el.attrib.get('href')
                    if 'timesofindia.indiatimes.com' not in link:
                        continue
                    text = el.text
                    rows.append({
                        "date": cd.strftime("%Y-%m-%d"),
                        "text": text,
                        "link": link,
                        "source": "Times of India"
                    })
                pandas.DataFrame(rows).to_csv(_path, index=False, encoding="utf8")
                



def extract_section_hindu(x):
    for frag in x.split("/")[::-1]:
        if frag.startswith("tp-"):
            section = frag.replace("tp-", "")
            return section
    return "Front Page"

def scrape_hindu_new(config):
    # 2006 onwards new archive page
    sy, ey, sm, em, sd, ed = config
    ofs = OSFS("archives/hindu", create=True)
    for year in range(sy, ey + 1):
        yfs = ofs.makedir(str(year), recreate=True)
        for month in range(sm, em + 1):
            mfs = yfs.makedir(str(month), recreate=True)
            for day in range(sd, ed + 1):
                try:
                    cd = datetime(year, month, day)
                except ValueError:
                    continue

                dfs = mfs.makedir(str(day), recreate=True)

                _file = cd.strftime("%Y-%m-%d_manifest.csv")
                _path = dfs.getsyspath(_file)

                if dfs.exists(_file):
                    continue

                url = "https://www.thehindu.com/archive/print/%d/%02d/%02d/" % (year, month, day)
                print(url)
                resp = requests.get(url)
                doc = lxml.html.fromstring(resp.content)

                rows = []

                for el in doc.xpath("//ul[@class='archive-list']//li//a"):
                    link = el.attrib.get('href')
                    text = el.text
                    rows.append({
                        "date": cd.strftime("%Y-%m-%d"),
                        "text": text,
                        "link": link,
                        "section": extract_section_hindu(link),
                        "source": "The Hindu"

                    })
                pandas.DataFrame(rows).to_csv(_path, index=False, encoding="utf8")

                
def scrape_hindu_old(config):
    # till 2005 -- different view for archives
    sy, ey, sm, em, sd, ed = config
    ofs = OSFS("archives/hindu", create=True)
    for year in range(sy, ey + 1):
        yfs = ofs.makedir(str(year), recreate=True)
        for month in range(sm, em + 1):
            mfs = yfs.makedir(str(month), recreate=True)
            for day in range(sd, ed + 1):
                try:
                    cd = datetime(year, month, day)
                except ValueError:
                    continue

                dfs = mfs.makedir(str(day), recreate=True)

                _file = cd.strftime("%Y-%m-%d_manifest.csv")
                _path = dfs.getsyspath(_file)

                if dfs.exists(_file):
                    continue

                url = "https://www.thehindu.com/%d/%02d/%02d/99hdline.htm" % (year, month, day)
                resp = requests.get(url)
                doc = lxml.html.fromstring(resp.content)

                print(url)

                if not resp.status_code == requests.codes.ok:
                    continue

                sectionhead = None
                rows = []
                sel = doc.xpath('//td[@width=380]')[0]
                for child in sel.getchildren():
                    try:
                        content = child.text_content()
                    except ValueError:
                        continue

                    if child.tag in ["table", "p"]:
                        sectionhead = ''.join(child.itertext()).strip()

                    if child.tag in ['div', 'li']:
                        frag = child.xpath(".//a")[0]
                        rows.append({
                            "date": cd.strftime("%Y-%m-%d"),
                            "text": frag.text_content(),
                            "link": urljoin(url, frag.attrib['href']),
                            "section": sectionhead,
                            "source": "The Hindu"
                        })

                        sh = child.xpath(".//table")
                        if sh:
                            sectionhead = ''.join(sh[0].itertext()).strip()

                print(url, len(rows))

                pandas.DataFrame(rows).to_csv(_path, index=False, encoding="utf8")

def scrape_indian_express(config):
    # 2006 onwards new archive page
    sy, ey, sm, em, sd, ed = config
    ofs = OSFS("archives/indian_express", create=True)
    for year in range(sy, ey + 1):
        yfs = ofs.makedir(str(year), recreate=True)
        for month in range(sm, em + 1):
            mfs = yfs.makedir(str(month), recreate=True)
            for day in range(sd, ed + 1):
                try:
                    cd = datetime(year, month, day)
                except ValueError:
                    continue

                dfs = mfs.makedir(str(day), recreate=True)

                _file = cd.strftime("%Y-%m-%d_manifest.csv")
                _path = dfs.getsyspath(_file)

                if dfs.exists(_file):
                    continue

                url = "http://archive.indianexpress.com/archive/news/%d/%d/%d/" % (day, month, year)
                print(url)
                resp = requests.get(url)
                doc = lxml.html.fromstring(resp.content)

                rows = []
                section = None
                for el in doc.xpath("//div[@id='box_330'] | //div[@id='box_330_rt']"):
                    try:
                        section = el.xpath("./h4")[0].text
                    except:
                        pass

                    for art in el.xpath(".//a"):
                        text = art.tail
                        link = art.attrib.get("href")
                        if link.startswith("/"):
                            link = urljoin(url, link)

                        rows.append({
                            "date": cd.strftime("%Y-%m-%d"),
                            "text": text,
                            "link": link,
                            "section": section,
                            "source": "The Indian Express"
                        })
                
                pandas.DataFrame(rows).to_csv(_path, index=False, encoding="utf8")

def download_url(config):
    folder, url = config
    _hash = hashlib.md5(url.encode("utf-8")).hexdigest()
    _hash = "%s.html" % _hash
    ofs = OSFS(folder)

    if ofs.exists(_hash):
        return
    

    try:
        resp = requests.get(url, timeout=10)
    except SSLError:
        try:
            resp = requests.get(url, verify=False)
        except:
            return
    except Exception:
        print("Error: %s" % url)
        return

    print("\t\t\tDownloaded... %s" % url)

    with ofs.open(_hash, "wb") as f:
        f.write(resp.content)

                
p = Pool(32)

# hindu old
chunks = []

d1 = date(2002, 1, 1)  # start date
d2 = date(2005, 12, 31)  # end date

while d1 <= d2:
    _d = d1 + timedelta(days=30*4)
    if _d > d2:
        _d = d2
    chunks.append(
        [d1.year, _d.year, d1.month, _d.month, d1.day, _d.day]
    ) 
    d1 = _d + timedelta(days=1)

p.map(scrape_hindu_old, chunks)

# hindu_new
chunks = []

d1 = date(2006, 1, 1)  # start date
d2 = date.today()  # end date

while d1 <= d2:
    _d = d1 + timedelta(days=30*4)
    if _d > d2:
        _d = d2
    chunks.append(
        [d1.year, _d.year, d1.month, _d.month, d1.day, _d.day]
    ) 
    d1 = _d + timedelta(days=1)
p.map(scrape_hindu_new, chunks)


# # toi
# chunks = []

# d1 = date(2001, 1, 1)  # start date
# d2 = date.today()  # end date

# while d1 <= d2:
#     _d = d1 + timedelta(days=30*4)
#     if _d > d2:
#         _d = d2
#     chunks.append(
#         [d1.year, _d.year, d1.month, _d.month, d1.day, _d.day]
#     ) 
#     d1 = _d + timedelta(days=1)
# p.map(scrape_toi, chunks)


# # indian express
# chunks = []

# d1 = date(1997, 5, 1)  # start date
# d2 = date(2014, 2, 28)  # end date

# while d1 <= d2:
#     _d = d1 + timedelta(days=30*4)
#     if _d > d2:
#         _d = d2
#     chunks.append(
#         [d1.year, _d.year, d1.month, _d.month, d1.day, _d.day]
#     ) 
#     d1 = _d + timedelta(days=1)

# p.map(scrape_indian_express, chunks)


# # Combining files
# ofs = OSFS("archives")
# dfs = []
# for f in ofs.walk.files(filter=["*manifest.csv"]):
#     print(f)
#     _p = ofs.getsyspath(f)
#     try:
#         _df = pandas.read_csv(_p)
#     except pandas.errors.EmptyDataError:
#         continue

#     for row in _df.to_dict(orient="records"):
#         print row
#         raise


#     dfs.append(_df)

# pandas.concat(dfs, sort=True).to_csv("links.csv", index=False, encoding="utf8")

import sys
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
    for f in ofs.walk.files(filter=["*manifest.csv"]):
        _p = ofs.getsyspath(f)
        folder = fs.path.combine(fs.path.dirname(_p), "_pages")
        _fs = OSFS(folder, create=True)
        try:
            _df = pandas.read_csv(_p)
        except pandas.errors.EmptyDataError:
            continue

        tasks = []

        for row in _df.to_dict(orient="records"):
            c += 1

            if not row['link'].startswith("http"):
                continue

            tasks.append([
                folder, row['link']
            ])

        print("%s / %s" % (c, tc))
        p.map(download_url, tasks)