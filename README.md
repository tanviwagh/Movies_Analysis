# Movies_Analysis



## Overview



## Architecture Diagram
![Architecture Diagram](https://raw.githubusercontent.com/tanviwagh/Movies_Analysis/pre-prod/images/ArchitectureDiagram.png)


## Tech Stack

#### Big Data Frameworks
- PySpark 
- Hive

#### Python Libraries
- requests
- boto3
- Beautiful Soup 
- IMDbPY

#### AWS
- EMR
- S3
- Athena 
- QuickSight

#### Pipeline Orchestration 
- Apache Airflow


## Project Directory Structure
```
Movie_Analysis
├─ .github
│  └─ ISSUE_TEMPLATE
│     ├─ story.md
│     └─ task.md
├─ .gitignore
├─ app
│  ├─ code
│  │  ├─ create_bucket.py
│  │  ├─ data_cleaning.py
│  │  ├─ data_quality_check.py
│  │  ├─ get_movie_data.py
│  │  ├─ schema_creation.py
│  │  └─ __init__.py
│  ├─ conf
│  │  ├─ config.yml
│  │  └─ __init__.py
│  ├─ data
│  ├─ parquet
│  ├─ run.py
│  ├─ utils
│  │  ├─ helper.py
│  │  └─ s3_helper.py
│  └─ __init__.py
├─ dags
│  └─ movie_data_dag.py
├─ images
│  ├─ movie_data_dag.PNG
│  └─ movie_ER.jpg
├─ LICENSE
├─ main.py
├─ README.md
└─ run.sh

```

## Files Description 
1. **config.yml** - configuration file
2. **main.py** - main file to run all the modules
3. **get_movie_data.py** - extracts recent movie data from using IMDbPY
4. **data_cleaning.py** -  preprocesses the data using PySpark
5. **schema_creation.py** - creates schema for tables in AWS Athena
6. **data_quality_check.py** - performs the following checks:-
    - row count check
    - null values check
    - table exists check
7. **helper.py** -  general purpose helper functions 
8. **s3_helper.py** - AWS S3 helper functions 
9. **movie_data_dag.py** - DAG which runs on every Friday to extract recently 
released movies data


## Setup


## License
MIT
