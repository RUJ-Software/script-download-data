from urllib.request import urlopen
import xml.etree.ElementTree as ET

links = []
counter = 0


def searchEntries(url):
    url_response = urlopen(url) # return a http.response object
    new_link = ""
    global counter

    tree = ET.parse(url_response)
    root = tree.getroot()

    entry_tag = '{http://www.w3.org/2005/Atom}entry'

    for element in root:
        if element.tag == entry_tag:
            #links.append(element.find('{http://www.w3.org/2005/Atom}link').attrib['href'])
            pass
        if 'rel' in element.attrib and element.attrib['rel'] == 'next':
            new_link=element.attrib['href']

    if new_link:
        counter = counter + 1
        print(counter)
        searchEntries(new_link)
    else:
        print('Search Finished')


url = "https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3.atom"
searchEntries(url)
print(len(links))
