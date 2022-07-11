import os
import requests
import pandas as pd


def get_items_data_from_api():
    """
    1. Using the code from the lesson as a guide and the REST API from https://python.zgulde.net/api/v1/items 
    as we did in the lesson, create a dataframe named items that has all of the data for items.
    """
    domain = 'https://api.data.codeup.com'
    #data origin
    endpoint = '/api/v1/items'
    #what we want specifically
    items = []
    #building an empty list to fill later
    while True:
        #always happening
        url = domain + endpoint
        response = requests.get(url)
        data = response.json()
        print(f'\rGetting page {data["payload"]["page"]} of {data["payload"]["max_page"]}: {url}', end='')
        #message to indicate that function is running and current progress
        items.extend(data['payload']['items'])
        endpoint = data['payload']['next_page']
        if endpoint is None:
            break
    items = pd.DataFrame(items)
    return items

def get_store_data_from_api():
    """
    2. Do the same thing, but for stores (https://python.zgulde.net/api/v1/stores)
    """
    response = requests.get('https://api.data.codeup.com/api/v1/stores')
    #target the website that we want information from
    data = response.json()
    #we take that object as a json format
    stores = pd.DataFrame(data['payload']['stores'])
    #pulling just payload and stores
    stores = pd.DataFrame(stores)
    #and now have all of the store data we want
    return stores


def get_sales_data_from_api():
    """
    3. Extract the data for sales (https://python.zgulde.net/api/v1/sales). There are a lot of pages of data here, 
    so your code will need to be a little more complex. Your code should continue fetching data from the next page until all of the data is extracted.
    """
    base_url = 'https://api.data.codeup.com/api/v1/sales?page='
    sales = []
    url = base_url + str(1)
    response = requests.get(url)
    data = response.json()
    max_page = data['payload']['max_page']
    #last page as end point
    sales.extend(data['payload']['sales'])
    page_range = range(2, max_page + 1)

    for page in page_range:
        url = base_url + str(page)
        print(f'\rFetching page {page}/{max_page} {url}', end='')
        response = requests.get(url)
        data = response.json()
        sales.extend(data['payload']['sales'])
    sales = pd.DataFrame(sales)
    return sales

#the following items are to check for csv's first
def get_stores_data():
    if os.path.exists('stores.csv'):
        return pd.read_csv('stores.csv')
    df = get_store_data_from_api()
    df.to_csv('stores.csv', index=False)
    return df

def get_items_data():
    if os.path.exists('items.csv'):
        return pd.read_csv('items.csv')
    df = get_items_data_from_api()
    df.to_csv('items.csv', index=False)
    return df

def get_sales_data():
    if os.path.exists('sales.csv'):
        return pd.read_csv('sales.csv')
    df = get_sales_data_from_api()
    df.to_csv('sales.csv', index=False)
    return df

def get_store_item_demand_data():
    sales = get_sales_data()
    stores = get_stores_data()
    items = get_items_data()

    sales = sales.rename(columns={'store': 'store_id', 'item': 'item_id'})
    df = pd.merge(sales, stores, how='inner', left_on='store_id', right_on='store_id')
    df = pd.merge(df, items, how='inner', left_on='item_id', right_on='item_id')

    return df

def get_opsd_data():
    if os.path.exists('opsd.csv'):
        return pd.read_csv('opsd.csv')
    df = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
    df.to_csv('opsd.csv', index=False)
    return df