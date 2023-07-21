curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.6.3/docker-compose.yaml'

rm flower

mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env

docker compose up airflow-init

docker compose up -d

Go to http://localhost:8080
user : airflow
mdp : airflow