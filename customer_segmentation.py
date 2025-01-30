import numpy as np
import pandas as pd
import pickle as pkl
import json
import os
from sql_utils import get_engine, make_read_query


LIFETIME_COLS = ['active_days', 'inactive_days', 'avg_days_to_order', 
                'items_per_order', 'avg_order_value', 'segment', 'churn_flag']

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

def add_churn_flags(repeat_purchasers_clustered):
    cluster_churn_limits = repeat_purchasers_clustered.groupby('cluster')\
                                                      .avg_days_to_order.quantile(0.9)\
                                                      .to_frame('churn_limit')
    cluster_upcoming_churn_limits = repeat_purchasers_clustered.groupby('cluster')\
                                                                .avg_days_to_order.quantile(0.75)\
                                                                .to_frame('upcoming_churn_limit')
    repeat_purchasers_clustered = repeat_purchasers_clustered.join(cluster_churn_limits, on='cluster')\
                                                             .join(cluster_upcoming_churn_limits, on='cluster')
    upcoming_churn_idx = repeat_purchasers_clustered['inactive_days'] > repeat_purchasers_clustered['upcoming_churn_limit']
    churn_idx = repeat_purchasers_clustered['inactive_days'] > repeat_purchasers_clustered['churn_limit']
    repeat_purchasers_clustered.loc[upcoming_churn_idx, 'churn_flag'] = 'Upcoming Churn'
    repeat_purchasers_clustered.loc[churn_idx, 'churn_flag'] = 'Churned'
    return repeat_purchasers_clustered

def concat_lifetime_cols(repeat_purchasers, df_lifetime_flagged):
    return  pd.concat([repeat_purchasers, 
                       df_lifetime_flagged.drop(columns=['churn_limit', 'upcoming_churn_limit'])],
                      axis=1)

def concat_customers(repeat_purchasers_clustered, one_off_customers, inactive_users):
    repeat_purchasers_clustered = repeat_purchasers_clustered.rename(columns={'cluster': 'segment'})
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
    users_enriched = users_enriched.drop(columns=LIFETIME_COLS)

    repeat_purchasers, one_off_customers, inactive_users = split_customers(users_enriched)
    now = users_enriched.last_purchase_date.max()

    one_off_customers_lifetime = make_lifetime_features(one_off_customers, now)
    one_off_customers = one_off_customers.join(one_off_customers_lifetime)
    inf_days_to_order_mask = one_off_customers.avg_days_to_order==np.inf
    one_off_customers.loc[inf_days_to_order_mask, 'avg_days_to_order'] = None
    
    cust_segment_pipe, cust_segment_map = load_cust_segment_pipe()
    cust_segment_map = {int(k): v for k,v in cust_segment_map.items()}

    repeat_purchasers_lifetime = make_lifetime_features(repeat_purchasers, now)
    repeat_purchasers_lifetime['cluster'] = cust_segment_pipe.predict(repeat_purchasers_lifetime)
    repeat_purchasers_lifetime['cluster'] = repeat_purchasers_lifetime['cluster'].map(cust_segment_map)
    repeat_purchasers_flagged = add_churn_flags(repeat_purchasers_lifetime)
    repeat_purchasers_clustered = concat_lifetime_cols(repeat_purchasers, repeat_purchasers_flagged)

    customers_segmented = concat_customers(repeat_purchasers_clustered, one_off_customers, inactive_users)

    with engine.connect() as conn:
        customers_segmented.to_sql('users_enriched', conn, 
                                   if_exists='replace', index=False)


if __name__ == '__main__':
    segment_customers()