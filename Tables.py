# PURPOSE: his file extracts data from multiple PTDC databsaes and their tables and converts them into single database.
# sqlalchemy is the library used for performing read and write operations with MySql

import pandas as pd
import numpy as np
import sqlalchemy


# Converts columns of bill_items to most efficient data types as per their size.
# The data types for id related columns might have to be changed later if length of the table increases.
# We can also do it for other dataframes too if we wish to speed up the execution time.

def change_bill_items_type(bill_items_data):
    bill_items_data['bill_id'] = bill_items_data['bill_id'].astype(np.uint16)
    # bill_items_data['bill_uid'] = bill_items_data['bill_uid'].astype(np.uint64)
    bill_items_data['bill_item_id'] = bill_items_data['bill_item_id'].astype(np.uint32)
    # bill_items_data['bill_item_uid'] = bill_items_data['bill_item_uid'].astype(np.uint64)
    bill_items_data['quantity'] = bill_items_data['quantity'].astype(np.float32)
    bill_items_data['price'] = bill_items_data['price'].astype(np.float32)
    bill_items_data['product_id'] = bill_items_data['product_id'].astype(np.uint16)
    # bill_items_data['product_description'] = bill_items_data['product_description'].astype()
    return bill_items_data


# This function reads bill table from a given database and append it to bill_data dataframe after adding bill_uid as unique column.
def join_bill(bill_data, year, month, engine):
    data = pd.read_sql('bill_201' + str(year) + '_' + str(month), engine)
    data['bill_uid'] = 10000000 * year + 100000 * month + data['bill_id']
    bill_data = bill_data.append(data, ignore_index=True)
    return bill_data


# This function reads bill_items table from a given database and append it to bill_items_data dataframe after adding bill_uid and bill_item_uid as unique columns.
def join_bill_items(bill_items_data, year, month, engine):
    data = pd.read_sql('bill_items_201' + str(year) + '_' + str(month), engine,
                       columns=['bill_item_id', 'quantity', 'price', 'product_id', 'bill_id',
                                'product_description'])
    data['bill_uid'] = (10000000 * year + 100000 * month + data['bill_id']).astype(np.uint32)
    data['bill_item_uid'] = (100000000 * year + 1000000 * month + data['bill_item_id']).astype(np.uint32)
    data = change_bill_items_type(data)
    bill_items_data = bill_items_data.append(data, ignore_index=True)
    return bill_items_data


# This function appends stock_category from a given database to stock_category_data dataframe and removes duplicates
def create_stock_category(stock_category_data, engine):
    data = pd.read_sql('stock_category', engine)
    stock_category_data = stock_category_data.append(data)
    stock_category_data.drop_duplicates(subset=['category_id', 'cateogory_code'], keep='last', inplace=True)
    return stock_category_data


# This function appends stock_product from a given database to stock_product_data dataframe and removes duplicates
def create_stock_product(stock_product_data, engine):
    data = pd.read_sql('stock_product', engine)
    stock_product_data = stock_product_data.append(data)
    stock_product_data.drop_duplicates(subset=['product_id', 'product_code'], keep='last', inplace=True)
    return stock_product_data


bill_data = pd.DataFrame()
bill_items_data = pd.DataFrame()
stock_product_data = pd.DataFrame()
stock_category_data = pd.DataFrame()

# To read all databaases from year 2013-18
for year in range(3, 9):
    year_begin = year
    year_end = year + 1

    # Because 2018 DB follows a different format.
    if year > 7:
        database_name = 'invent'
        month_begin_1 = 4
        month_end_1 = 11
        month_begin_2 = 1
        month_end_2 = 1
    else:
        database_name = 'invent_201' + str(year_begin) + '_201' + str(year_end)
        month_begin_1 = 4
        month_end_1 = 13
        month_begin_2 = 1
        month_end_2 = 4
    engine = sqlalchemy.create_engine('mysql://root:root@localhost:3306/' + database_name)

    # Because DB has tables from April-March
    for month in range(month_begin_1, month_end_1):
        bill_data = join_bill(bill_data, year_begin, month, engine)
        bill_items_data = join_bill_items(bill_items_data, year_begin, month, engine)
    for month in range(month_begin_2, month_end_2):
        bill_data = join_bill(bill_data, year_end, month, engine)
        bill_items_data = join_bill_items(bill_items_data, year_end, month, engine)
        stock_product_data = create_stock_product(stock_product_data, engine)
        stock_category_data = create_stock_category(stock_category_data, engine)
        # print year, len(stock_category_data), len(stock_product_data)
        # print stock_category_data.dtypes
        # print stock_product_data.dtypes
        # print bill_data.dtypes
        # print bill_items_data.dtypes

# Removing single duplicate entry that was still left.
stock_product_data = stock_product_data[
    (stock_product_data.product_id != 2356) | (stock_product_data.product_code != '559')]
# print stock_product_data[stock_product_data['product_id'] == 2356]

# duplicate_products = stock_product_data[stock_product_data.duplicated(subset='product_id',keep=False)]
# duplicate_products.to_csv('duplicate_products.csv')

# To make index start from 1
bill_data.index += 1
bill_items_data.index += 1


# Writing to a MySQL DB one table at a time and defining their data types
engine = sqlalchemy.create_engine('mysql://root:root@localhost:3306/ptdc_local')
stock_category_data.to_sql('stock_category', engine, index=False, dtype={'category_id': sqlalchemy.types.SMALLINT(),
                                                                         'category_code': sqlalchemy.types.VARCHAR(
                                                                             length=16),
                                                                         'category_description': sqlalchemy.types.VARCHAR(
                                                                             length=255),
                                                                         'parent_category_id': sqlalchemy.types.SMALLINT(),
                                                                         'is_perishable': sqlalchemy.types.CHAR(
                                                                             length=1),
                                                                         'is_modified': sqlalchemy.types.CHAR(
                                                                             length=1)})
stock_product_data.to_sql('stock_product', engine, index=False, dtype={'product_id': sqlalchemy.types.INT(),
                                                                       'product_code': sqlalchemy.types.VARCHAR(
                                                                           length=16),
                                                                       'product_bar_code': sqlalchemy.types.VARCHAR(
                                                                           length=13),
                                                                       'product_description': sqlalchemy.types.VARCHAR(
                                                                           length=255),
                                                                       'is_av_product': sqlalchemy.types.CHAR(
                                                                           length=1),
                                                                       'measurement_unit_id' : sqlalchemy.types.SMALLINT(),
                                                                       'category_id': sqlalchemy.types.SMALLINT()})

bill_data.to_sql('bill', engine, index=True,
                 dtype={'index': sqlalchemy.types.INT(), 'bill_id': sqlalchemy.types.INT(),
                        'date_created': sqlalchemy.types.DATETIME(),
                        'bill_uid': sqlalchemy.types.INT(), 'total_amount': sqlalchemy.types.FLOAT(),
                        'account_number': sqlalchemy.types.VARCHAR(length=6),
                        'account_name': sqlalchemy.types.VARCHAR(length=50)})


bill_items_data.to_sql('bill_items', engine, index=True, chunksize=5000,
                       dtype={'index': sqlalchemy.types.INT(), 'bill_item_id': sqlalchemy.types.INT(),
                              'quantity': sqlalchemy.types.FLOAT(),
                              'price': sqlalchemy.types.FLOAT(), 'product_id': sqlalchemy.types.INT(),
                              'bill_id': sqlalchemy.types.INT(),
                              'product_description': sqlalchemy.types.VARCHAR(length=255),
                              'bill_uid': sqlalchemy.types.INT(), 'bill_item_uid': sqlalchemy.types.INT()})
# #
# # accounts_data = pd.read_sql('account_pt', engine)
