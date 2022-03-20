# Movies_Analysis



## Overview



## Architecture Diagram


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
2. **get_movie_data.py** - 
3. **data_cleaning.py** -  
4. **schema_creation.py** - 
5. **data_quality_check.py** - 
6. **helper.py** -  
7. **s3_helper.py** 
8. **movie_data_dag.py** -


## Setup


## License
MIT
