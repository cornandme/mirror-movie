from datetime import datetime
from datetime import timedelta
import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from airflow_config import airflow_config

root_path = airflow_config['MIRROR-MOVIE']['ROOT_PATH']

with open(f"{root_path}/config.json") as f:
    config = json.load(f)



default_args = {
    'owner': config['AIRFLOW']['OWNER'],
    'depends_on_past': False,
    'email': config['AIRFLOW']['EMAIL'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    dag_id='mirror-movie_rec',
    default_args=default_args,
    description='DAG for batch process',
    start_date=datetime(2021, 5, 29),
    schedule_interval='0 19 * * *',
    tags=['rec'],
) as dag:

    t1 = BashOperator(
        task_id='movie_info_scraper',
        bash_command=f"python3 {root_path}/scrapers/movie_info_scraper.py -root_path {root_path}",
    )

    t2 = BashOperator(
        task_id='user_review_scraper',
        bash_command=f"python3 {root_path}/scrapers/user_review_scraper.py -root_path {root_path}",
    )

    t3 = BashOperator(
        task_id='movie_info_preprocess',
        depends_on_past=True,
        bash_command=f"python3 {root_path}/processors/movie_info_preprocess.py -root_path {root_path}",
    )

    t4 = BashOperator(
        task_id='do_tokenize',
        depends_on_past=True,
        bash_command=f"python3 {root_path}/processors/do_tokenize.py -root_path {root_path}",
    )

    t5 = BashOperator(
        task_id='get_morphs',
        depends_on_past=True,
        bash_command=f"python3 {root_path}/processors/get_morphs.py -root_path {root_path}",
    )

    t6 = BashOperator(
        task_id='do_tokenize_okt',
        depends_on_past=True,
        bash_command=f"python3 {root_path}/processors/do_tokenize_okt.py -root_path {root_path}",
    )

    t7 = BashOperator(
        task_id='get_morphs_okt',
        depends_on_past=True,
        bash_command=f"python3 {root_path}/processors/get_morphs_okt.py -root_path {root_path}",
    )

    t8 = BashOperator(
        task_id='get_representation',
        depends_on_past=True,
        bash_command=f"python3 {root_path}/processors/get_representation.py -root_path {root_path}",
    )

    t9 = BashOperator(
        task_id='clustering',
        depends_on_past=True,
        bash_command=f"python3 {root_path}/processors/clustering.py -root_path {root_path}",
    )

    t10 = BashOperator(
        task_id='recommender',
        depends_on_past=True,
        bash_command=f"python3 {root_path}/processors/recommender.py -root_path {root_path}",
    )

    t11 = BashOperator(
        task_id='keyword_extraction',
        depends_on_past=True,
        bash_command=f"python3 {root_path}/processors/keyword_extraction.py -root_path {root_path}",
    )

    t12 = BashOperator(
        task_id='search_rec',
        depends_on_past=True,
        bash_command=f"python3 {root_path}/processors/search_rec.py -root_path {root_path}",
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11 >> t12