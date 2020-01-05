# rey : ML and Pre-Processing Automation

rey is the workflow manager which used Airflow to automate all the Machine Learning and Data Pre-processing pipelines in our system. Some of the steps in our pipeline are
* Fetching and indexing raw data and entities which are in the bucket
* Economic News Detection (To-be-done)
* Text Extraction - By hitting the news URL and fetching data
* Some cleaning, indexing and preprocessing - Probably storing it in a relational database
* Sentiment Analysis
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

### Running
`docker-compose up --build`

### Deployment
* delete .data and run `docker-compose up --build postgres`
* `docker-compose up --build initdb`
* and no airflow.cfg and unittests

## References

* [Configuring Airflow in docker-compose](https://medium.com/@xnuinside/quick-guide-how-to-run-apache-airflow-cluster-in-docker-compose-615eb8abd67a)
* [Best Practices](https://gtoonstra.github.io/etl-with-airflow/principles.html)
* [Config](https://github.com/kjam/data-pipelines-course/issues/1)