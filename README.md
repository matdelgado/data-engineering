# Data Engineering

Welcome to the data engineering project!

## Prerequisites

Before getting started, you will need to install the following software on your machine:

* [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)  
* [Docker Desktop](https://www.docker.com/products/docker-desktop/)  
* [VSCode](https://code.visualstudio.com/)

Since the installation process depends on your operating system, you’ll need to handle this part on your own.
However, I've added some guidance for [Windows](./_support/windows.md), [macOS](./_support/macos.md) and [Ubuntu](./_support/ubuntu.md).

You also need to create a GitHub account to follow along with the project. Create your account at [https://github.com/](https://github.com/).

## Cloning the Project Repository

First, you need to access the original project repository at https://github.com/weslleymoura/data-engineering and create a **fork**. This will make a copy of the project in your own GitHub account (as a new repository).

<img src="_support/git-fork.png" width="400">

**After forking**, open your terminal and navigate to the **directory where you want to save the project** (throughout the project, we will refer to this directory as the **working dir**).

Next, clone your forked project:

```
git clone <<your-repository-url>>
```

To get your project URL, go to the GitHub repository you just forked (in your GitHub account) and copy the following address (HTTPS):

<img src="_support/git-clone.png" width="400">


## Starting the project

Please, execute the following commands to initiate the project.

#### Working directory

Before you start, make sure you are inside the project directory (data-engineering).

#### Grant permissions

Make sure you granted full access on Airflow and DBT folders

```
sudo chmod -R 777 airflow/
sudo chmod -R 777 dbt_lakehouse/
```

#### Build all services

Use the following command to build all services.

```
docker compose up -d --build
```

The first run downloads several GB of images and builds three of them, so it can take
15-30 minutes depending on your connection. Give Docker Desktop at least **8 GB of RAM**
(Settings → Resources), otherwise Spark and Superset will be killed on startup.

Follow the progress with:

```
docker compose ps
docker compose logs -f airflow
```

#### Initial setup of the data catalog

```
docker exec -it spark-master bash -c "/scripts/update_hive_metastore.sh"
```

#### Checking if everyting is okay

Your project is correct if you are able to access the following links from your localhost.

| Service | URL | User | Password |
| --- | --- | --- | --- |
| MinIO Console UI | http://localhost:9001 | `minio` | `minio123` |
| Airflow Web UI | http://localhost:8080 | `admin` | `admin` |
| Spark Master UI | http://localhost:8081 | – | – |
| Apache Superset UI | http://localhost:8088 | `admin` | `admin` |

These are throwaway credentials for a local teaching environment — never reuse them
outside of it.

## Notes for project configurations (no need to execute anything)

Please, take note of the following project configurations 

#### Superset connection string

Use this connection string to connect Superset to the lakehouse.

```
hive://spark-thrift-server:10000/default
```

Once connected, you should be able to query the DBT table

```
SELECT SUM(total_amount) AS total_amount FROM marts.fct_summary
```



#### Initiating DBT Docs server

```
docker exec -it airflow bash -c "cd /home/airflow/dbt_lakehouse/target && python3 -m http.server 8091"
```

Check results at http://localhost:8091


## Support commands (run as needed)

#### Access airflow container as root user.

```
docker exec -u 0 -it airflow bash
```

#### Adjust system permissions

For example, if you need to grant full access on airflow folder.

```
chmod -R 777 airflow/
```

#### Run DBT project from Airflow container
```
docker exec -it airflow bash -c "dbt run --project-dir /home/airflow/dbt_lakehouse"
```

#### Run DBT model from Airflow container
```
docker exec -it airflow bash -c "dbt run --project-dir /home/airflow/dbt_lakehouse --select models/marts/fct_summary.sql"
```

#### Connecting to the Hive metastore from spark-master

```
docker exec -it spark-master bash
beeline -u jdbc:hive2://spark-thrift-server:10000
show schemas;
show tables in default;
show tables in marts;
```

#### Shutting down docker containers

You can use the following command to destroy all services.

```
docker compose down --volumes --remove-orphans
```

In case you have lost refence to the existing containers, it is possible to force a shut down.

```
docker container kill $(docker container ls -q)
```

> ⚠️ This kills **every** running container on your machine, not only this project's.

## Troubleshooting

#### Checking container logs

Whatever the problem is, the reason is almost always in the last few lines of the
service's log:

```
docker compose logs --tail=50 airflow
```

#### `Bind for 0.0.0.0:8080 failed: port is already allocated`

Another process (often an unrelated container) is already using one of the ports
this project publishes: `8080`, `8081`, `8088`, `8091`, `9000`, `9001`, `7077`,
`10000`, `10001`. Find the culprit and stop it:

```
docker ps --filter "publish=8080"
lsof -nP -iTCP:8080 -sTCP:LISTEN
```

#### Resolving permissions issue

In case Airflow is failing because of permission issues.

```
sudo chmod -R 777 airflow/
sudo chmod -R 777 dbt_lakehouse/
```
