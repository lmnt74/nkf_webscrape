import pandas as pd
import time

def createpropertydf (final_property, property, type):
    try:
        # property dataframe
        max_id = final_property['id'].max()
        lease_df = pd.DataFrame(property).T
        lease_df.insert(0, 'id', int(max_id + 1))
        lease_df.columns = final_property.columns
        final_property = final_property.append(lease_df, sort=False)
    except Exception as e:
        # property dataframe
        print('exception is:', e)
        lease_df = pd.DataFrame(property).T
        lease_df.insert(0, 'id', 1)
        final_property = final_property.append(lease_df, sort=False)
    if type == "lease":
        lease_df.columns = ['id', 'url', 'address', 'city', 'state', 'region', 'market', 'submarket', 'total_sqft_available'
            , 'spaces']
    elif type == "sale":
        lease_df.columns = ['id', 'url', 'address', 'city', 'state', 'region', 'market', 'submarket', 'description']
    return final_property


def createbrokerdf (final_broker_table, final_property, int_broker_table):
    try:
        max_id = final_property['id'].max()
        # broker dataframe
        broker_df = pd.DataFrame(int_broker_table)
        broker_df.insert(2, 'property_id', int(max_id))
        broker_df.columns = ['brokerName', 'brokerPhoneNumber', 'PropertyID']
        final_broker_table = final_broker_table.append(broker_df)
    except Exception as e:
        print(e)
    return final_broker_table