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
link = "https://newmarkretail.com/lease-property-list/?from=details_search&region=New%20York%" \
       "20Metro&market&sub_market&total_space_min=MIN%20SIZE&total_space_max=MAX%20SIZE&space" \
       "_search_type&submit=Search"

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

## uncomment the below when using locally
# with open('./file_lease.txt', 'r') as file:
#     content = file.read()
#
# soup = BeautifulSoup(content, 'html.parser')
# print(soup)
##

tables = soup.find_all('table', class_="lease_property")

final_lease_property = pd.DataFrame()
final_broker_table = pd.DataFrame()

for gparent in tables:
    print('g\n', gparent)
    # appending row
    lease_property = []
    if gparent.tr or gparent.td:
        for parent in gparent.children:
            print('parent::: ', parent)
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
                        int_broker_table = pd.DataFrame()
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
    try:
        # property dataframe
        max_id = final_lease_property['id'].max()
        lease_df = pd.DataFrame(lease_property).T
        lease_df.insert(0, 'id', int(max_id+1))
        print(lease_df)
        lease_df.columns = final_lease_property.columns
        final_lease_property = final_lease_property.append(lease_df, sort=False)
        # broker dataframe
        broker_df = pd.DataFrame(int_broker_table)
        broker_df.insert(2, 'property_id', int(max_id+1))
        print(broker_df)
        final_broker_table = final_broker_table.append(broker_df)
    except Exception as e:
        # property dataframe
        print('exception is:', e)
        lease_df = pd.DataFrame(lease_property).T
        lease_df.columns = ['url', 'address', 'city', 'state', 'region', 'market', 'submarket', 'total_sqft_available'
            , 'spaces']
        lease_df.insert(0, 'id', 1)
        print(lease_df)
        final_lease_property = final_lease_property.append(lease_df, sort=False)
        # broker dataframe
        broker_df = pd.DataFrame(int_broker_table)
        broker_df.insert(2, 'property_id', 1)
        print(broker_df)
        final_broker_table = final_broker_table.append(broker_df)

print('final broker --** \n', final_broker_table)
print('final property --** \n', final_lease_property)

# todo need to put it into sql database