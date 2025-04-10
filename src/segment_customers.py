import numpy as np
import pandas as pd
import pickle as pkl
import json
import os
from sql_utils import get_engine, make_read_query_func


CLUSTERING_FEATURES = ['n_orders', 'avg_days_to_order', 'avg_order_items', 'avg_item_value']

def load_cust_segment_pipe(model_path='../models/'):
    pipe_path = os.path.join(model_path, 'cust_segment_pipe.pkl')
    map_path = os.path.join(model_path, 'cust_segment_map.json')

    with open(pipe_path, 'rb') as f:
        cust_segment_pipe = pkl.load(f)

    with open(map_path, 'r') as f:
        cust_segment_map = json.load(f)

    return cust_segment_pipe, cust_segment_map

def segment_customers():
    # Read data
    engine = get_engine()
    read_query = make_read_query_func(engine)
    users_enriched = read_query("SELECT * FROM users_enriched", verbose=False)
    init_cols = users_enriched.columns

    # Split inactive, one-off and repeat customers
    inactive_users = users_enriched.loc[users_enriched.n_orders==0].copy()
    one_off_customers = users_enriched.loc[users_enriched.n_orders==1].copy()
    repeat_purchasers = users_enriched.loc[users_enriched.n_orders>1].copy()

    # Add segments for inactive and one-off customers
    one_off_customers['segment'] = 'One-Off Purchasers'
    inactive_users['segment'] = 'Never Ordered'

    # Cluster repeat customers & add churn status
    cust_segment_pipe, cust_segment_map = load_cust_segment_pipe()
    cust_segment_map = {int(k): v for k,v in cust_segment_map.items()}

    X = repeat_purchasers[CLUSTERING_FEATURES]
    repeat_purchasers['segment'] = cust_segment_pipe.predict(X)
    repeat_purchasers['segment'] = repeat_purchasers['segment'].map(cust_segment_map)

    # Concat all customers
    customers_segmented = pd.concat([repeat_purchasers, one_off_customers, inactive_users])\
                            .sort_values('created_at')[init_cols]

    # Write to DB
    with engine.connect() as conn:
        customers_segmented.to_sql('users_enriched', conn, 
                                   if_exists='replace',
                                   index=False)


if __name__ == '__main__':
    segment_customers()
