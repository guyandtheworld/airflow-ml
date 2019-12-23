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

`docker-compose up initdb` 


## References

* [Configuring Airflow in docker-compose](https://medium.com/@xnuinside/quick-guide-how-to-run-apache-airflow-cluster-in-docker-compose-615eb8abd67a)
* [Best Practices](https://gtoonstra.github.io/etl-with-airflow/principles.html)
