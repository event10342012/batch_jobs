import abc

import requests
from bs4 import BeautifulSoup


class BaseSpider(metaclass=abc.ABCMeta):
    """
    :param url: url to fetch HTML
    """

    def __init__(self, url):
        self.name = None
        self.__url = url
        self._soup = None

    @property
    def soup(self):
        return self._soup

    @property
    def url(self):
        return self.__url

    def fetch(self, params=None):
        response = requests.get(self.url, params=params)
        self._soup = BeautifulSoup(response.text, 'lxml')

    @abc.abstractmethod
    def parse(self, *args, **kwargs):
        pass
