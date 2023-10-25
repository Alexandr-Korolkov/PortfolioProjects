#загрузка библиотек
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta, date

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#параметры подключения
connection = {'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
                     }

#параметры ДАГа Airflow
default_args = {
    'owner': 'a-korolkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 10),
}

#расписание ДАГа
schedule_interval = '0 11 * * *'

#функция получения датафрейма
def get_df(query, connection):
    df = ph.read_clickhouse(query=query, connection=connection)
    return df


#ДАГ
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def korolkov_telegram_report():
    #пишем запрос в БД
    @task()
    def get_data_1(connection):
        query_1 = '''
        SELECT CAST(DAO AS INT) as DAO,
              CAST(uniq_posts AS INT) as uniq_posts,
              ROUND(ctr, 2) As ctr,
              CAST(all_actions AS INT) as all_actions,
              CAST(view_per_user AS INT) as view_per_user,
              CAST(likes_per_user AS INT) as likes_per_user
        FROM 
        (SELECT uniq (user_id) DAO,
              uniq (post_id ) AS  uniq_posts,
              countIf(action ='like')AS  like ,
              countIf(action ='view')AS  view ,
              like+view AS all_actions,
              like/DAO as likes_per_user,
              view/DAO as view_per_user,
              like/view AS ctr
        FROM simulator_20230920.feed_actions 
        WHERE toDate(time)=yesterday() )
        '''
        data_1 = get_df(query_1, connection)
        return data_1

#получение необходимых метрик и создание шаблона для формирования регулярного отчета
    @task()
    def text(data_1):
        dau = data_1.DAO.values[0]
        uniq_posts = data_1.uniq_posts.values[0]
        ctr = data_1.ctr.values[0]
        all_actions = data_1.all_actions.values[0].round(2)
        view_per_user = data_1.view_per_user.values[0]
        likes_per_user = data_1.likes_per_user.values[0]

        message = '''Ключевые показатели за вчера:   
        DAU: {},
        Количество постов: {},
        CTR: {},
        Все действия пользователей: {},
        Просмотры на 1 человека: {}
        Лайки на 1 человека: {}'''.format(dau, uniq_posts, ctr, all_actions, view_per_user, likes_per_user)

        return message

#дополнительный запрос
    @task()
    def get_data_2(connection):
        query_2 = '''
        SELECT toDate(time) AS Date,
              uniq (user_id) DAO,
              countIf(action ='like')AS  like ,
              countIf(action ='view')AS  view ,
              like/view AS ctr
        FROM simulator_20230920.feed_actions 
        WHERE toDate(time) BETWEEN  today()-7 AND today()-1
        GROUP BY toDate(time)
        '''
        data_2 = get_df(query_2, connection)
        return data_2

#формирование шаблона для отправки графика
    @task()
    def plot(data_2):
        sns.set_style("whitegrid")
        figure, axes = plt.subplots(2, 2, figsize=(10, 6))
        plt.subplots_adjust(wspace=0.2, hspace=0.4)
        figure.suptitle('Динамика ключевых показателей за последнюю 1 нед.')
        sns.lineplot(ax=axes[0, 0], data=data_2, x=data_2.Date, y=data_2.DAO).tick_params(axis='x', rotation=30)
        sns.lineplot(ax=axes[0, 1], data=data_2, x=data_2.Date, y=data_2.like).tick_params(axis='x', rotation=30)
        sns.lineplot(ax=axes[1, 0], data=data_2, x=data_2.Date, y=data_2.view).tick_params(axis='x', rotation=30)
        sns.lineplot(ax=axes[1, 1], data=data_2, x=data_2.Date, y=data_2.ctr).tick_params(axis='x', rotation=30)

        axes[0, 0].legend(title="DAO", loc=3)
        axes[0, 0].set(xlabel='', ylabel='')
        axes[0, 1].legend(title="Likes", loc=3)
        axes[0, 1].set(xlabel='', ylabel='')
        axes[1, 0].legend(title="Views", loc=3)
        axes[1, 0].set(xlabel='', ylabel='')
        axes[1, 1].legend(title="CTR", loc=3)
        axes[1, 1].set(xlabel='', ylabel='')

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot.png'
        plt.close()
        return plot_object


#функция для отправка шаблона сообщения и графика в телеграм
    @task()
    def load(massage,plot_object):
        chat_id = -938659451
        my_token = '6378450216:AAGGzhXUVeZqT5S1bcQUWOkB1jkXWtfWC1w'
        bot = telegram.Bot(token=my_token)

        bot.sendMessage(chat_id=chat_id, text=massage, parse_mode='Markdown')
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

#необходмые связи
    data_1 = get_data_1(connection)
    massage = text(data_1)
    data_2 = get_data_2(connection)
    plot_object = plot(data_2)
    load(massage, plot_object)

korolkov_telegram_report = korolkov_telegram_report()
