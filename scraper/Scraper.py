from bs4 import BeautifulSoup
import requests

class Scraper(object):
  def __init__(self, url):
    self.url = url
    print("Descargando...")
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'lxml')
    self._raw_data = soup.select('form.form')
    print(self._raw_data)
    return self.raw_data

  def getData(self):
    pass
    