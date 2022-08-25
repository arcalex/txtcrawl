import time
from langdetect import detect
import os
import configargparse
from multiprocessing import Pool
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
import codecs
import math
import datetime
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter
import requests

# Add configuration file and arguments for different modes of operation
visited = {}
my_parser = configargparse.ArgParser(default_config_files=['txtcrawl.conf'])
my_parser.add_argument('-c', '--config', is_config_file=True, help='config file')
my_parser.add_argument('--seeds', help='file containing list of uris to crawl')
my_parser.add_argument('--seeds_from_warc', help='file to write seeds collected from warc to')
my_parser.add_argument('--use_warc_outlinks', action='store_true',
                       help='use outlinks from payloads in addition to warc record uris as seeds')
my_parser.add_argument('--extract', help='extract text from WARC file')
my_parser.add_argument('--level', type=int, help='depth of a crawl, 0 for unlimited')
my_parser.add_argument('--txt_dir', help='base directory to write TXT files to')
my_parser.add_argument('--wet_dir', help='base directory to write WET files to')
my_parser.add_argument('--crawl_log', help='crawl.log file')
my_parser.add_argument('--nprocs', type=int, help='number of parallel processes when crawling')
options = my_parser.parse_args()


# Saving files in a directory tree by splitting the url
def create_dir(body, url, abs_path=options.txt_dir):
    split_url = url.split('//')
    split_url = split_url[1].split('/')
    path = abs_path

    for i in range(len(split_url) - 1):
        path = os.path.join(path, split_url[i])

        try:
            os.mkdir(path)
        except FileExistsError:
            continue

    txt_file_name = split_url[-1].replace('?', '')

    with open(f"{path}/{txt_file_name}.txt", 'a+', encoding='utf-8') as f:
        f.write(body)
        f.close()


# Saving files in a WET format (not fully functional)
def create_wet_file(url):
    with open(f"BigData/test1.wet.gz", 'wb+') as wet:
        writer = WARCWriter(wet, gzip=True)

        try:
            response = requests.get(url, headers={'Accept-Encoding': "identity, gzip", 'Content-Type':
                'text/html; charset=utf-8'}, stream=True)
            headers_list = response.raw.headers.items()
            headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')
            record = writer.create_warc_record(uri=url, record_type='response', payload=response.raw, http_headers=headers)
            writer.write_record(record)
        except requests.exceptions.ConnectionError as exception:
            print(exception)


# Writing a log file to update the user of what's going on
def write_log_file(parent_link, child_link, length, status_code):
    with open(options.crawl_log, 'a+') as log:
        log.write(f"{datetime.datetime.now()} {status_code} {length} {child_link} - {parent_link} text/html - - - - -\n"
                  )
        log.close()


def process_pool(file):
    urls = open(file, "r")
    pool = Pool(int(options.nprocs))
    pool.map(crawl, urls)
    pool.close()
    pool.join()


# Extracting seeds from a WARC file
def seeds_from_warc(input_file):
    warc_files = open(input_file, 'r')

    for warc in warc_files:
        warc = warc.strip('\n')

        with open(warc, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type == 'response':  # Response is the record type for HTML/txt files
                    url = record.rec_headers.get_header('WARC-Target-URI')

                    if options.seeds_from_warc:
                        with open("seeds.txt", 'a+') as seeds:
                            seeds.write(f'{url}\n')

                    if options.use_warc_outlinks:
                        payload = record.raw_stream.read()
                        soup = BeautifulSoup(payload, 'lxml')

                        # If page is encoded in windows-1256 force encoding to be utf-8 so the browser could render
                        # Arabic text clearly
                        if soup.original_encoding == 'windows-1256':
                            text = codecs.decode(obj=payload, encoding='utf-8', errors='strict')
                            soup = BeautifulSoup(text, 'lxml')

                        if options.extract:
                            create_dir(soup.get_text, url, options.txt_dir)

                        # Find outlinks in archived HTML page and save them along with target URI
                        for soup in soup.find_all('a'):
                            outlink = soup.get('href')

                            if outlink:
                                if outlink[:4] == 'http':
                                    seeds.write(f'{outlink}\t\t\t outlink\n')
                                    seeds.close()

    warc_files.close()


# Crawl through all seeds provided in a text file with a user-specified level
def crawl(url, hops=int(options.level)):
    if hops == 0:
        hops = math.inf

    for _ in range(hops):
        response = requests.get(url, headers={'Accept-Encoding': 'identity', 'Content-Type': 'text/html; charset=utf-8',
                                        'User-agent': 'Mozilla/5.0'}, stream=True)
        write_log_file(url, 'NA', 'NA', response.status_code)

        # If a page doesn't exist or is forbidden, write the error in a log file and go to the next url
        if response.status_code != 200:
            break

        # Extract text from HTML page
        soup = BeautifulSoup(response.content, 'lxml')
        body = soup.body

        for string in body.strings:
            try:
                lang = detect(body)
                if lang == "ar":
                    if options.txt_dir:
                        create_dir(body, url)
                    elif options.wet_dir:
                        create_wet_file(url)
                        break
                else:
                    with open("langdetect.txt", "a+") as lang:
                        lang.write(url)
                        lang.close()
                    continue
            except:
                continue

        for soup in soup.find_all('a'):
            soup = soup.get('href')

            if soup:
                if soup[:4] == 'http':
                    if soup not in visited.keys():
                        visited.update({soup: hops})
                        crawl(soup)
                        hops -= 1


if __name__ == '__main__':
    start = time.time()

    if options.seeds:
        process_pool(options.seeds)
    elif options.seeds_from_warc:
        seeds_from_warc(options.seeds_from_warc)
    elif options.extract:
        seeds_from_warc(options.extract)
    else:
        print("Error: Please enter an option of either seeds or seeds_from_warc")

    print(time.time() - start)
