from urllib.request import urlopen
import xml.etree.ElementTree as ET
from scraper import Scraper as sc
from sparkStreaming import SparkStreaming

links = []
counter = 0
url = "https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3.atom"
counter = 0
prev_url = ""

ss = SparkStreaming()


while url != None:
    url_response = urlopen(url) # return a http.response object
    counter

    tree = ET.parse(url_response)
    root = tree.getroot()

    entry_tag = '{http://www.w3.org/2005/Atom}entry'

    for element in root:
        if element.tag == entry_tag:
            new_link = element.find('{http://www.w3.org/2005/Atom}link').attrib['href']
            links.append(new_link)
            scraper = sc.Scraper(new_link)
            ss.send_raw_data(scraper)
        if 'rel' in element.attrib and element.attrib['rel'] == 'next':
            prev_url = url
            url=element.attrib['href']

    if prev_url != url and counter < 1:
        counter = counter + 1
        print(counter)
        print(f"There are {len(links)} links")
    else:
        url = None
        print('Search Finished')


print(len(links))
