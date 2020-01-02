from bs4 import BeautifulSoup
import pandas as pd
import sqlalchemy
import os
import requests
import time
import copy
import numpy as np
import config
from functions import pullrequests as pr, dataframecreation as dfc

# todo make this an env variable (for airflow docker)
# link = os.environ.get('newmarkLeaseLink')

## comment out below when running locally
link = "https://newmarkretail.com/sale-property-list/"

##
# key = pr.pullkey(link)
# print('key:', key)
# headers = config.headers(key)
# print('headers:', headers)
# soup = pr.createsoup(link, headers)
##

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

## uncomment whenever running locally
with open('file_sale.html', 'r') as file:
    content = file.read()
soup = BeautifulSoup(content, 'html.parser')
# print(soup)
##

tables = soup.find_all('table', class_="sale_property")
final_sale_property = pd.DataFrame()
final_sale_broker_table = pd.DataFrame()

for gparent in tables:
    # appending row
    sale_property = []
    int_broker_table = pd.DataFrame()
    if gparent.tr or gparent.td:
        for parent in gparent.children:
            if parent != '\n':
                for child in parent.children:
                    if child != '\n':
                        try:
                            if child.select('.lease_property_detail_link_text'):
                                # url_listing
                                if child.a.get('href') not in sale_property:
                                    sale_property.append(child.a.get('href'))
                                # listing_address
                                if child.a.get_text().split("|")[0].strip() not in sale_property:
                                    sale_property.append(child.a.get_text().split("|")[0].strip())
                                # listing city
                                if child.a.get_text().split("|")[1].strip().split(",")[0] not in sale_property:
                                    sale_property.append(child.a.get_text().split("|")[1].strip().split(",")[0])
                                # listing state
                                if child.a.get_text().split("|")[1].strip().split(",")[1] not in sale_property:
                                    sale_property.append(child.a.get_text().split("|")[1].strip().split(",")[1])
                            if child.select('.region') and child.select(".region")[0].get_text() not in sale_property:
                                sale_property.append(child.select(".region")[0].get_text())
                            # submarket idiosyncracies:
                            # print(child.select(".sub_market"))
                            for value in child.select(".sub_market"):
                                sale_property.append(value.get_text())
                            if child.select(".total_available"):
                                sale_property.append(child.select(".total_available")[0].get_text())
                            for gchild in child.select('.broker_table'):
                                for broker in gchild.select("tr"):
                                    if broker != '\n':
                                        broker_table = []
                                        if broker.select('.broker_contact_name')[0].get_text() \
                                                not in broker_table:
                                            broker_table.append(
                                                broker.select('.broker_contact_name')[0].get_text())
                                        if broker.select('.broker_primary_phone')[0].get_text() \
                                                not in broker_table:
                                            broker_table.append(
                                                broker.select('.broker_primary_phone')[0].get_text())
                                        int_broker_table = int_broker_table.append(pd.DataFrame(broker_table).T)
                            # time.sleep(3)
                        except Exception as e:
                            print('Exception is:', e)
    final_sale_property = dfc.createpropertydf(final_sale_property, sale_property, type="sale")
    final_sale_broker_table = dfc.createbrokerdf(final_sale_broker_table, final_sale_property, int_broker_table)

print('final broker --** \n', final_sale_broker_table)
print('final property --** \n', final_sale_property)

# todo need to put it into sql database (make sure it's distinct)