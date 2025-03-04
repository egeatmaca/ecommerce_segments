import numpy as np
import pandas as pd
from datetime import datetime as dt
from sql_utils import get_engine, make_read_query_func


GLOBAL_CHURN_LIM = 365

def flag_churn(now='now'):
    # Read data
    engine = get_engine()
    read_query = make_read_query_func(engine)
    users_enriched = read_query("SELECT * FROM users_enriched", verbose=False)
    init_cols = users_enriched.columns
    
    # Get the timestamp for now
    if now == 'now':
        now = dt.now()
    elif now == 'pseudo':
        now = users_enriched.last_order_timestamp.max()

    # Calculate inactive days
    users_enriched['inactive_days'] = (now - users_enriched.last_order_timestamp).dt.days

    # Get limits for inactivity anomalies based on user history
    user_churn_lims = users_enriched.avg_days_to_order + 2 * users_enriched.std_days_to_order

    # Flag churn
    users_enriched['lifetime_status'] = 'Active'
    churn_mask = ((users_enriched.inactive_days > user_churn_lims) |
                  (users_enriched.inactive_days > GLOBAL_CHURN_LIM))
    users_enriched.loc[churn_mask, 'lifetime_status'] = 'Churned'
    users_enriched.loc[users_enriched.n_orders==0, 'lifetime_status'] = 'Inactive'

    # Write to DB
    with engine.connect() as conn:
        users_enriched.to_sql('users_enriched', conn, 
                              if_exists='replace',
                              index=False)


if __name__ == '__main__':
    flag_churn(now='pseudo')