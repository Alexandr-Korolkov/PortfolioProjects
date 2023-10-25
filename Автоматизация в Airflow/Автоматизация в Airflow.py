#Импорт библиотек
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta, date
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#параметры подключения к базам данных
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                        'database': 'simulator',
                        'user': 'student',
                        'password': 'dpo_python_2020'
                       }
connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                   'database': 'test',
                   'user': 'student-rw',
                   'password': '656e2b0c9c'
                  }
#Параметры ДАГа для airflow
default_args = {
    'owner': 'a-korolkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 10),
}
#параметры расписания
schedule_interval = '0 23 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def korolkov_dag():
#запросы для получения датафреймов
    @task()
    def query_feed(connection):
        query_1 = '''
        SELECT toDate(time) date,
              user_id,
              os,
              gender,
              age,
              countIf(action = 'like') likes,
              countIf(action = 'view') views
        FROM simulator_20230920.feed_actions 
        WHERE toDate(time)= yesterday()
        GROUP BY user_id, date, os, gender, age
        '''
        df_1 = ph.read_clickhouse(query=query_1, connection=connection)
        return df_1

    @task()
    def query_massage(connection):
        query_2 = '''
        SELECT t1.date, user AS user_id, messages_sent,users_sent,messages_received,users_received, os,gender,age
        FROM 
        (SELECT toDate(time) date, 
              user_id AS user,
              COUNT(user_id) messages_sent,
              uniq(receiver_id) users_sent,
              os,
              gender,
              age
        FROM simulator_20230920.message_actions 
        WHERE toDate(time)= yesterday() 
        GROUP BY user_id, os, gender, age, date)t1
        
        FULL JOIN
        
        (SELECT toDate(time) date, 
              receiver_id as user,
              COUNT(receiver_id) messages_received,
              uniq(user_id) users_received
        FROM simulator_20230920.message_actions 
        WHERE toDate(time)= yesterday() 
        GROUP BY receiver_id, date)t2
        
        ON t1.user=t2.user
        
        WHERE date = yesterday()
        '''
        df_2 = ph.read_clickhouse(query=query_2, connection=connection)
        return df_2
#соединяем датафреймы
    @task()
    def merge(df_1, df_2):
        df = pd.merge(df_1, df_2, on="user_id",how ='outer')
        return df
#редактируем датафрейм, объединяем, переназываем,убираем лишние колонки
    @task()
    def transform_1(df):
        df.date_x[df.date_x.isnull()]= df.date_y[df.date_x.isnull()]
        df.os_x[df.os_x.isnull()]= df.os_y[df.os_x.isnull()]
        df.gender_x[df.gender_x.isnull()]= df.gender_y[df.gender_x.isnull()]
        df.age_x[df.age_x.isnull()]= df.age_y[df.age_x.isnull()]
        df.fillna(0, inplace=True)
        df_t1=df[['date_x','user_id','os_x','gender_x','age_x','likes','views','messages_sent','users_sent','messages_received','users_received']]
        df_t1.columns=df_t1.columns = ['event_date','user_id','os','gender','age','likes','views','messages_sent','users_sent','messages_received','users_received']
        return df_t1
#группируем данные
    @task()
    def transform_2(df_t1):
        main=pd.DataFrame()
        p1=df_t1[['likes','views','messages_sent','users_sent','messages_received','users_received']].groupby([df_t1.age,df_t1.event_date]).sum()
        p1['dimension']='age'
        p1.index.rename(['dimension_value','event_date'], inplace= True )
        main = pd.concat([p1, main])
        p2=df_t1[['likes','views','messages_sent','users_sent','messages_received','users_received']].groupby([df_t1.os,df_t1.event_date]).sum()
        p2['dimension']='os'
        p2.index.rename(['dimension_value','event_date'], inplace= True )
        main = pd.concat([p2, main])
        p3=df_t1[['likes','views','messages_sent','users_sent','messages_received','users_received']].groupby([df_t1.gender,df_t1.event_date]).sum()
        p3['dimension']='gender'
        p3.index.rename(['dimension_value','event_date'], inplace= True )
        main = pd.concat([p3, main])
        return main
#убираем колонки, реиндексируем
    @task()
    def transform_3(main):
        main_t3 = main.reset_index().set_index('event_date')[['dimension','dimension_value','views','likes','messages_sent','users_sent','messages_received','users_received']]
        return main_t3
#меняем формат колонок для последующей выгрузки в БД
    @task()
    def transform_4(main_t3):
        main_t4=main_t3.reset_index().copy()
        main_t4['dimension_value'] = main_t4['dimension_value'].astype('str')
        main_t4['views'] = main_t4['views'].astype('int')
        main_t4['likes'] = main_t4['likes'].astype('int')
        main_t4['messages_received'] = main_t4['messages_received'].astype('int')
        main_t4['messages_sent'] = main_t4['messages_sent'].astype('int')
        main_t4['users_received'] = main_t4['users_received'].astype('int')
        main_t4['users_sent'] = main_t4['users_sent'].astype('int')
        main_t4['event_date'] = main_t4['event_date'].dt.date
        return main_t4
#создаем запрос на выгрузку датафрейма
    @task
    def load(connection_test, main_t4):
        query = '''
        CREATE TABLE IF NOT EXISTS test.korolkov_table (
            event_date Date,
            dimension String,
            dimension_value String,
            views Int64,
            likes Int64,
            messages_sent Int64,
            users_sent Int64,
            messages_received Int64,
            users_received Int64)
        ENGINE = MergeTree()
        ORDER BY event_date
        '''

        ph.execute(query, connection=connection_test)
        ph.to_clickhouse(main_t4, 'korolkov_table', index=False, connection=connection_test)
# строим необходимые связи и запускаем ДАГ
    df_1=query_feed(connection)
    df_2=query_massage(connection)
    df=merge(df_1, df_2)
    df_t1=transform_1(df)
    main=transform_2(df_t1)
    main_t3=transform_3(main)
    main_t4 = transform_4(main_t3)
    load(connection_test, main_t4)


korolkov_dag = korolkov_dag()
