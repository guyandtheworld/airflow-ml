# airflow-ml : ML and Pre-Processing Automation

This is a workflow manager which uses Airflow to automate all the Machine Learning and Data Pre-processing pipelines in our system. Some of the most important steps in our pipeline are
* Fetching and indexing raw data and entities which are in the bucket
* Economic News Detection (To-be-done)
* Text Extraction - By hitting the news URL and fetching data
* Some cleaning, indexing and preprocessing - Probably storing it in a relational database
* Sentiment Analysis
* Fuzzy Matching
* Entity Extraction
* Custom Scoring Model (BERT)

## Development

Airflow needs persistent storage to store the details of all the executions and pipelines. Right now, we run the postgres docker image as a temporary solution. We need to change it to production ready database.

When setting up airflow for the first time, we need to run the migrations which is in the docker-compose so that the tables can be setup.

* Set the $AIRFLOW_HOME to the current file root
  `export AIRFLOW_HOME=$(pwd)`
* airflow.cfg shouldn't be copied
* remove the default airflow.cfg while setting up locally


### Testing Locally

* `airflow webserver`
* `airflow backfill indexing -s 2019-12-30`


`airflow test indexing test_entities 2020-01-01`
`airflow test indexing test_entities 2020-01-01`



### Migrations: Run only the first time for setting up tables

`docker-compose up initdb` 

`docker-compose up --build`

* delete .data and run `docker-compose up --build postgres`
* `docker-compose up --build initdb`
* and no airflow.cfg and unittest
* User Auth
* Disable auto-start `docker update --restart=no rey_worker_1 rey_scheduler_1 rey_webserver_1 rey_flower_1 rey_redis_1 rey_postgres_1`
```
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__WEBSERVER__RBAC=True
```

* If you fuck up db

    - `airflow resetdb`
    - `docker exec -it rey_scheduler_1 /entrypoint.sh bash`

* Creating a New User

    `airflow create_user -r Admin -u dev -e adarsh@alrt.ai -f username -l s -p password`

* Configuring Fernet Key for Production

    Fetch the Fernet key and update initdb to make it work properly (using docker exec)
    `export FERNET_KEY = python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)"`

## Distributing Workers

MetaStore: `postgresql+psycopg2://airflow:airflow@postgres:5432/airflow`
BrokerURL: `redis://redis:6379/1`

For running workers on different nodes, connect to the metastore db and the redis queue by configuring
the docker-compose with `POSTGRES_HOST` and `REDIS_HOST`. Also remove the dependency to the local scheduler


## Nginx configuration

* delete `/etc/nginx/sites-enabled/default`
* create `/etc/nginx/conf.d/reverse-proxies.conf`
* add A Record	airflow <IP>
* ```server {
        listen 80 default_server;
        listen [::]:80 default_server;
        server_name airflow.alrt.ai;

        location / {
              proxy_pass http://localhost:8080/;
              proxy_http_version 1.1;
              proxy_set_header Upgrade $http_upgrade;
              proxy_set_header Connection 'upgrade';
              proxy_set_header Host $host;
              proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
              proxy_cache_bypass $http_upgrade;
              proxy_set_header X-Forwarded-Proto $scheme;
        }
}
```


## References

* [Configuring Airflow in docker-compose](https://medium.com/@xnuinside/quick-guide-how-to-run-apache-airflow-cluster-in-docker-compose-615eb8abd67a)
* [Best Practices](https://gtoonstra.github.io/etl-with-airflow/principles.html)
* [Config](https://github.com/kjam/data-pipelines-course/issues/1)
* [Fernet Key and Stuff](https://medium.com/@itunpredictable/apache-airflow-on-docker-for-complete-beginners-cf76cf7b2c9a)
