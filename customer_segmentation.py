import numpy as np
import pandas as pd
import pickle as pkl
import json
import os
from sql_utils import get_engine, make_read_query


LIFETIME_COLS = ['active_days', 'inactive_days', 'avg_days_to_order', 
                'items_per_order', 'avg_order_value', 
                'segment', 'segment_churn_limit', 'churn_status']
CLUSTERING_COLS = ['active_days', 'avg_days_to_order', 'avg_order_value']

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

def make_lifetime_features(users_enriched, now):
    df_lifetime = users_enriched.copy()
    df_lifetime['active_days'] = (df_lifetime.last_purchase_date - df_lifetime.first_purchase_date).dt.days + 1
    df_lifetime['inactive_days'] = (now - df_lifetime.last_purchase_date).dt.days
    df_lifetime['avg_days_to_order'] = df_lifetime['active_days'] / (df_lifetime['n_orders'] - 1)
    df_lifetime['items_per_order'] = df_lifetime['n_order_items'] / df_lifetime['n_orders']
    df_lifetime['avg_order_value'] = df_lifetime['revenue'] / df_lifetime['n_orders']
    df_lifetime = df_lifetime[['active_days', 'inactive_days', 'avg_days_to_order', 'items_per_order', 'avg_order_value']]
    return df_lifetime

def add_churn_status(df_lifetime_cluster, percentile=0.9):
    cluster_churn_limits = df_lifetime_cluster.groupby('cluster')\
                                              .avg_days_to_order.quantile(percentile)\
                                              .to_frame('churn_limit')
    df_lifetime_cluster = df_lifetime_cluster.join(cluster_churn_limits, on='cluster')
    churn_idx = df_lifetime_cluster['inactive_days'] > df_lifetime_cluster['churn_limit']
    df_lifetime_cluster['churn_status'] = 'Active'
    df_lifetime_cluster.loc[churn_idx, 'churn_status'] = 'Churn Likely'
    return df_lifetime_cluster

def concat_customers(repeat_purchasers_clustered, one_off_customers, inactive_users):
    new_col_names = {'cluster': 'segment', 'churn_limit': 'segment_churn_limit'}
    repeat_purchasers_clustered = repeat_purchasers_clustered.rename(columns=new_col_names)
    one_off_customers, inactive_users = one_off_customers.copy(), inactive_users.copy()
    one_off_customers['segment'] = 'One-Off Customers'
    inactive_users['segment'] = 'Never Ordered'
    customers_segmented = pd.concat([repeat_purchasers_clustered, one_off_customers, inactive_users])\
                            .sort_values('created_at')
    return customers_segmented

def segment_customers():
    engine = get_engine()
    read_query = make_read_query(engine)

    users_enriched = read_query("SELECT * FROM users_enriched", verbose=False)

    # Split inactive, one-off and repeat customers
    repeat_purchasers, one_off_customers, inactive_users = split_customers(users_enriched)

    # Pseudo-now for lifetime calculations
    now = users_enriched.last_purchase_date.max()

    # Calculate lifetime features for one-off customers
    one_off_customers = one_off_customers.drop(columns=LIFETIME_COLS)
    one_off_customers_lifetime = make_lifetime_features(one_off_customers, now)
    one_off_customers = one_off_customers.join(one_off_customers_lifetime)
    inf_days_to_order_mask = one_off_customers.avg_days_to_order==np.inf
    one_off_customers.loc[inf_days_to_order_mask, 'avg_days_to_order'] = None
    
    # Cluster repeat customers & add churn status
    cust_segment_pipe, cust_segment_map = load_cust_segment_pipe()
    cust_segment_map = {int(k): v for k,v in cust_segment_map.items()}

    repeat_purchasers = repeat_purchasers.drop(columns=LIFETIME_COLS)
    repeat_purchasers_lifetime = make_lifetime_features(repeat_purchasers, now)
    X = repeat_purchasers_lifetime[CLUSTERING_COLS]
    repeat_purchasers_lifetime['cluster'] = cust_segment_pipe.predict(X)
    repeat_purchasers_lifetime['cluster'] = repeat_purchasers_lifetime['cluster'].map(cust_segment_map)
    repeat_purchasers_lifetime = add_churn_status(repeat_purchasers_lifetime)
    repeat_purchasers_clustered = pd.concat([repeat_purchasers, repeat_purchasers_lifetime], axis=1)

    # Concat all customers
    customers_segmented = concat_customers(repeat_purchasers_clustered, one_off_customers, inactive_users)

    # Write to DB
    with engine.connect() as conn:
        customers_segmented.to_sql('users_enriched', conn, 
                                   if_exists='replace',
                                   index=False)


if __name__ == '__main__':
    segment_customers()