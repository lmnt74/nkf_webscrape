from bs4 import BeautifulSoup
import pandas as pd
import sqlalchemy
import os
import requests
import time
import copy
import numpy as np

# todo make this an env variable (for airflow docker)
# link = os.environ.get('newmarkLeaseLink')

## comment out below when running locally
link = "https://newmarkretail.com/sale-property-list/"

page = requests.get(link)
soup = BeautifulSoup(page.content, 'html.parser')
key = soup.find("div",class_="g-recaptcha")["data-sitekey"]
print(key)
time.sleep(5)

headers = {
    'User-Agent': 'Amit Shah',
    'From': 'amit.shah@gmail.com',
    'data-sitekey': '{}'.format(key)
}


page = requests.get(link, headers)
soup = BeautifulSoup(page.content, 'html.parser')
##

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

## uncomment whenever running locally
# with open('file_sale.html', 'r') as file:
#     content = file.read()
# soup = BeautifulSoup(content, 'html.parser')
# print(soup)
##

tables = soup.find_all('table', class_="sale_property")
final_sale_property = pd.DataFrame()
final_broker_table = pd.DataFrame()

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
    try:
        # property dataframe
        max_id = final_sale_property['id'].max()
        lease_df = pd.DataFrame(sale_property).T
        lease_df.insert(0, 'id', int(max_id+1))
        lease_df.columns = final_sale_property.columns
        final_sale_property = final_sale_property.append(lease_df, sort=False)
        # broker dataframe
        broker_df = pd.DataFrame(int_broker_table)
        broker_df.insert(2, 'property_id', int(max_id+1))
        final_broker_table = final_broker_table.append(broker_df)
    except Exception as e:
        # property dataframe
        print('exception is:', e)
        lease_df = pd.DataFrame(sale_property).T
        lease_df.columns = ['url', 'address', 'city', 'state', 'region', 'market', 'submarket', 'description']
        lease_df.insert(0, 'id', 1)
        final_sale_property = final_sale_property.append(lease_df, sort=False)
        # broker dataframe
        broker_df = pd.DataFrame(int_broker_table)
        broker_df.insert(2, 'property_id', 1)
        final_broker_table = final_broker_table.append(broker_df)


print('final broker --** \n', final_broker_table)
print('final property --** \n', final_sale_property)

# todo need to put it into sql database