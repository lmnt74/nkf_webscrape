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
link = "https://newmarkretail.com/lease-property-list/?from=details_search&region=New%20York%" \
       "20Metro&market&sub_market&total_space_min=MIN%20SIZE&total_space_max=MAX%20SIZE&space" \
       "_search_type&submit=Search"

##
key = pr.pullkey(link)
print('key:', key)
headers = config.headers(key)
print('headers:', headers)
soup = pr.createsoup(link, headers)
##

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

## uncomment the below when using locally
# with open('./file_lease.txt', 'r') as file:
#     content = file.read()
#
# soup = BeautifulSoup(content, 'html.parser')
# print(soup)
##

tables = soup.find_all('table', class_="lease_property")

final_lease_property = pd.DataFrame()
final_lease_broker_table = pd.DataFrame()

for gparent in tables:
    # print('g\n', gparent)
    # appending row
    lease_property = []
    int_broker_table = pd.DataFrame()
    if gparent.tr or gparent.td:
        for parent in gparent.children:
            # print('parent::: ', parent)
            try:
                for child in parent.children:
                    if child.select('.lease_property_detail_link_text'):
                        # url_listing
                        if child.a.get('href') not in lease_property:
                            lease_property.append(child.a.get('href'))
                        # listing_address
                        if child.a.get_text().split("|")[0].strip() not in lease_property:
                            lease_property.append(child.a.get_text().split("|")[0].strip())
                        # listing city
                        if child.a.get_text().split("|")[1].strip().split(",")[0] not in lease_property:
                            lease_property.append(child.a.get_text().split("|")[1].strip().split(",")[0])
                        # listing state
                        if child.a.get_text().split("|")[1].strip().split(",")[1] not in lease_property:
                            lease_property.append(child.a.get_text().split("|")[1].strip().split(",")[1])
                    if child.select('.region')\
                            and child.select(".region")[0].get_text() not in lease_property:
                        lease_property.append(child.select(".region")[0].get_text())
                    if child.select(".market"):
                        lease_property.append(child.select(".market")[0].get_text())
                    if child.select(".sub_market"):
                        lease_property.append(child.select(".sub_market")[0].get_text())
                    elif child.select(".sub_market"):
                        lease_property.append(np.NaN)
                    if child.select(".total_available"):
                        lease_property.append(child.select(".total_available")[0].get_text())
                    if child.select(".multiple_space_message") \
                            and child.select(".multiple_space_message")[0].get_text() not in lease_property:
                        lease_property.append(child.select(".multiple_space_message")[0].get_text())
                    for gchild in child.select('.broker_table'):
                        for broker in gchild:
                            if len(broker.select('.broker_contact_name'))>0:
                                for each_broker in broker:
                                    broker_table = []
                                    if each_broker.select('.broker_contact_name')[0].get_text() \
                                            not in broker_table:
                                        broker_table.append(each_broker.select('.broker_contact_name')[0].get_text())
                                    if each_broker.select('.broker_primary_phone')[0].get_text() \
                                            not in broker_table:
                                        broker_table.append(each_broker.select('.broker_primary_phone')[0].get_text())
                                    int_broker_table = int_broker_table.append(pd.DataFrame(broker_table).T)
            except Exception as e:
                print('Exception is: ', e)
    final_lease_property = dfc.createpropertydf(final_lease_property, lease_property, type="lease")
    final_lease_broker_table = dfc.createbrokerdf(final_lease_broker_table, final_lease_property, int_broker_table)

print('final broker --** \n', final_lease_broker_table)
print('final property --** \n', final_lease_property)

# todo need to put it into sql database