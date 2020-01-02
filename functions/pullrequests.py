import requests
from bs4 import BeautifulSoup

'''
Functions for the pull requests
'''


def pullkey (link):
    page = requests.get(link)
    soup = BeautifulSoup(page.content, 'html.parser')
    key = soup.find("div", class_="g-recaptcha")["data-sitekey"]
    return key


def createsoup (link, headers):
    page = requests.get(link, headers)
    soup = BeautifulSoup(page.content, 'html.parser')
    return soup
