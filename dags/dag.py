from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# إضافة مجلد المشروع إلى مسار بايثون حتى يستطيع Airflow رؤية مجلد kafka
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + '/..'))

# دالة وسيطة لاستخراج السنة والشهر من تاريخ Airflow
def trigger_producer(**kwargs):
    from kafka.producer import produce_taxi_data
    # data_interval_start هو المتغير الذي يحمل تاريخ المهمة الحالية في Airflow
    logical_date = kwargs['data_interval_start']
    year = logical_date.strftime("%Y")
    month = logical_date.strftime("%m") # سيعطي "01", "02" الخ
    
    produce_taxi_data(year, month)

default_args = {
    'owner': 'nyc_taxi',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
}

with DAG(
    'send_taxi_data_to_kafka',
    default_args=default_args,
    schedule_interval='@monthly', # سيعمل مرة كل شهر
    catchup=True, # هذه الكلمة السحرية ستجعل Airflow يشغل الأشهر السابقة تباعاً
    max_active_runs=1 # حتى لا يشغل الأشهر كلها في نفس اللحظة بل واحداً تلو الآخر
) as dag:

    task_produce_to_kafka = PythonOperator(
        task_id='produce_monthly_taxi_data',
        python_callable=trigger_producer,
        provide_context=True # ضروري لتمرير **kwargs
    )
