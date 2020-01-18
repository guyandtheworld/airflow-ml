TRY_LOOP="20"

# Defaults and back-compat
: "${AIRFLOW_HOME:="/usr/local/airflow"}"
: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
# : "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Sequential}Executor}"

# : "${POSTGRES_HOST:="postgres"}"
# : "${POSTGRES_PORT:="5432"}"
# : "${POSTGRES_USER:="airflow"}"
# : "${POSTGRES_PASSWORD:="airflow"}"
# : "${POSTGRES_DB:="airflow"}"

AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__BASE_URL=http://{hostname}:8080
AIRFLOW__WEBSERVER__RBAC=True

export \
  AIRFLOW_HOME \
  AIRFLOW__CORE__FERNET_KEY \
  AIRFLOW__CORE__LOAD_EXAMPLES \
  AIRFLOW__CORE__SQL_ALCHEMY_CONN \
  AIRFLOW__CORE__BASE_URL \
  AIRFLOW__WEBSERVER__RBAC \
#   AIRFLOW__CORE__EXECUTOR \
#   AIRFLOW__CELERY__BROKER_URL \
#   AIRFLOW__CELERY__RESULT_BACKEND \
