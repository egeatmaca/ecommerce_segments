FROM postgres:17.2

RUN apt-get update &&\
    apt-get install -y python3 python3-venv python3-pip unzip

COPY kaggle.json ~/.kaggle/kaggle.json

RUN python3 -m venv ./venv

RUN . ./venv/bin/activate &&\
    pip install kaggle &&\
    kaggle datasets download mustafakeser4/looker-ecommerce-bigquery-dataset

RUN unzip looker-ecommerce-bigquery-dataset.zip -d /docker-entrypoint-initdb.d

RUN rm -rf ~/.kaggle/kaggle.json venv looker-ecommerce-bigquery-dataset.zip

RUN apt-get purge --auto-remove -y python3 python3-venv python3-pip unzip

RUN mkdir /tmp/exports/ &&\
    chown postgres:postgres /tmp/exports/ &&\
    chmod 700 /tmp/exports