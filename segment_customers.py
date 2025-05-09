import numpy as np
import pandas as pd
import pickle as pkl
import json
import os
from sql_utils import get_engine, make_read_query_func


FEATURES = {
    'loyalty': ['n_orders', 'avg_days_to_order'],
    'order_value': ['avg_order_items', 'max_order_items', 'avg_item_value', 'max_item_value'],
}


def load_segmentation_pipe(pipe_name, model_path='./models/'):
    pipe_path = os.path.join(model_path, pipe_name+'_segment_pipe.pkl')
    map_path = os.path.join(model_path, pipe_name+'_segment_map.json')

    with open(pipe_path, 'rb') as f:
        segment_pipe = pkl.load(f)

    with open(map_path, 'r') as f:
        segment_map = json.load(f)

    return segment_pipe, segment_map

def segment_customers(repeat_purchasers, pipe_name):
    segment_pipe, segment_map = load_segmentation_pipe(pipe_name)
    segment_map = {int(k): v for k,v in segment_map.items()}

    repeat_purchasers = repeat_purchasers.copy() 
    X = repeat_purchasers[FEATURES[pipe_name]]

    segment_col = pipe_name+'_segment'
    repeat_purchasers[segment_col] = segment_pipe.predict(X)
    repeat_purchasers[segment_col] = repeat_purchasers[segment_col].map(segment_map)

    return repeat_purchasers

def main():
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
    one_off_customers['loyalty_segment'] = 'One-Off Purchasers'
    inactive_users['loyalty_segment'] = 'Never Ordered'

    # Cluster repeat purchasers
    repeat_purchasers = segment_customers(repeat_purchasers, 'loyalty')
    repeat_purchasers = segment_customers(repeat_purchasers, 'order_value')
    
    # Concat all customers
    customers_segmented = pd.concat([repeat_purchasers, one_off_customers, inactive_users])\
                            .sort_values('created_at')[init_cols]

    # Write to DB
    with engine.connect() as conn:
        customers_segmented.to_sql('users_enriched', conn, 
                                   if_exists='replace',
                                   index=False)


if __name__ == '__main__':
    main()
