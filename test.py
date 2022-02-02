from urllib.request import urlopen
import xml.etree.ElementTree as ET

url_response = urlopen("https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3.atom") # return a http.response object 

tree = ET.parse(url_response)
root = tree.getroot()

entry_tag = '{http://www.w3.org/2005/Atom}entry'

links = []

for element in root.iter(entry_tag):
    if element.tag == entry_tag:
        links.append(element.find('{http://www.w3.org/2005/Atom}link').attrib['href'])

print(len(links))
