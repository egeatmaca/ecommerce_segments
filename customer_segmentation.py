import numpy as np
import pandas as pd
import pickle as pkl
import json
import os
from sql_utils import get_engine, make_read_query


def load_cust_segment_pipe(model_path='./models/'):
    pipe_path = os.path.join(model_path, 'cust_segment_pipe.pkl')
    map_path = os.path.join(model_path, 'cust_segment_map.json')

    with open(pipe_path, 'rb') as f:
        cust_segment_pipe = pkl.load(f)

    with open(map_path, 'r') as f:
        cust_segment_map = json.load(f)

    return cust_segment_pipe, cust_segment_map

def split_customers(users_enriched):
    inactive_users = users_enriched.loc[users_enriched.n_orders==0]
    one_off_customers = users_enriched.loc[users_enriched.n_orders==1]
    repeat_purchasers = users_enriched.loc[users_enriched.n_orders>1]
    return repeat_purchasers, one_off_customers, inactive_users

def get_lifetime_features(users_enriched, now):
    now = now if now else dt.datetime.now()
    users_enriched = users_enriched.copy()
    users_enriched['inactive_days'] = (now - users_enriched.last_order_date).dt.days
    user_lifetimes = users_enriched[['active_days', 'inactive_days', 'avg_days_to_order', 'std_days_to_order', 'avg_order_items', 'avg_item_value']]
    return user_lifetimes

def add_churn_status(rp_lifetime_segments, percentile=0.9):
    churn_mask = rp_lifetime_segments['inactive_days'] > (rp_lifetime_segments['avg_days_to_order'] 
                                                         + 2 * rp_lifetime_segments['std_days_to_order'])
    rp_lifetime_segments['churn_status'] = 'Active'
    rp_lifetime_segments.loc[churn_mask, 'churn_status'] = 'Churn Likely'
    return rp_lifetime_segments

def concat_cols(df, df_cols):
    df = df.drop(columns=df_cols.columns)
    return  pd.concat([df, df_cols], axis=1)

def concat_customers(repeat_purchasers_segmented, one_off_customers, inactive_users):
    one_off_customers['segment'] = 'One-Off Customers'
    inactive_users['segment'] = 'Never Ordered'
    customers_segmented = pd.concat([repeat_purchasers_segmented, one_off_customers, inactive_users])\
                            .sort_values('created_at')
    return customers_segmented

def segment_customers():
    engine = get_engine()
    read_query = make_read_query(engine)

    users_enriched = read_query("SELECT * FROM users_enriched", verbose=False)

    # Split inactive, one-off and repeat customers
    repeat_purchasers, one_off_customers, inactive_users = split_customers(users_enriched)

    # Pseudo-now for lifetime calculations
    now = users_enriched.last_order_date.max()

    # Calculate lifetime features for one-off customers
    one_off_customers_lifetime = get_lifetime_features(one_off_customers, now)
    one_off_customers = concat_cols(one_off_customers, one_off_customers_lifetime)
    
    # Cluster repeat customers & add churn status
    cust_segment_pipe, cust_segment_map = load_cust_segment_pipe()
    cust_segment_map = {int(k): v for k,v in cust_segment_map.items()}

    repeat_purchasers_lifetime = get_lifetime_features(repeat_purchasers, now)
    X = repeat_purchasers_lifetime.drop(columns=['inactive_days', 'std_days_to_order'])
    repeat_purchasers_lifetime['segment'] = cust_segment_pipe.predict(X)
    repeat_purchasers_lifetime['segment'] = repeat_purchasers_lifetime['segment'].map(cust_segment_map)
    repeat_purchasers_lifetime = add_churn_status(repeat_purchasers_lifetime)
    repeat_purchasers_segmented = concat_cols(repeat_purchasers, repeat_purchasers_lifetime)

    # Concat all customers
    customers_segmented = concat_customers(repeat_purchasers_segmented, one_off_customers, inactive_users)

    # Write to DB
    with engine.connect() as conn:
        customers_segmented.to_sql('users_enriched', conn, 
                                   if_exists='replace',
                                   index=False)


if __name__ == '__main__':
    segment_customers()