from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'spidyweb',
    'retries': 0,
    'retry_delay': timedelta(seconds=20),
    'depends_on_past': False
}

dag_Spark_spidyweb = DAG(
    'dag_Spark_spidyweb_id',
    start_date=days_ago(2),
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    is_paused_upon_creation=False,
)

cmd='spark-submit /home/hadoop/pyspark_ETL_parquet.py'

#시작을 알리는 dummy
task_start = DummyOperator(
    task_id='start',
    dag=dag_Spark_spidyweb,
)

#시작이 끝나고 다음단계로 진행되었음을 나타내는 dummy
task_next = DummyOperator(
    task_id='next',
    trigger_rule='all_success',
    dag=dag_Spark_spidyweb,
)
#끝을 알리는 dummy
task_finish = DummyOperator(
    task_id='finish',
    trigger_rule='all_done',
    dag=dag_Spark_spidyweb,
)
#오늘의 목표인 bash를 통해 spark-submit할 operator
task_PySpark_1 = BashOperator(
    task_id='spark_submit_task',
    dag=dag_Spark_spidyweb,
    bash_command=cmd,
)
#의존관계 구성
task_start >> task_next >> task_PySpark_1 >> task_finish

#days_ago(2),days_ago(1),days_ago(0) 전부 돌고 datetime(2021,11,19)#오늘날짜로도 돈다.datetime(2021,11,18)#어제날짜로도 돈다 datetime(2021,11,20)#내일 날짜로 도 돈다.
#@daily  datetime(2021,11,20)#내일 날짜로 도 돈다. 
#SparkSubmitOperator는 내부적으로 spark binary인 spark-submit 을 호출하는 방식으로 spark을 실행하고 있다.
# 그러므로 Apache spark 에서 각자의 환경에 알맞은 spark을 각 airflow worker에 다운로드 한후 다운로드 경로를 $SPARK_HOME을 환경변수로 등록하고 $SPARK_HOME/bin 을 path에 추가한다.