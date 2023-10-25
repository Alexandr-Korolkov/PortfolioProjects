import pandahouse as ph
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

import io
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
                     }


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a-korolkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 10),
}


chat_id = -938659451
my_token = '6378450216:AAGGzhXUVeZqT5S1bcQUWOkB1jkXWtfWC1w'
bot = telegram.Bot(token=my_token)

schedule_interval = '58 10 * * *'  # отчет приходит каждый день в 11 утра


# Лента новостей

# DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def korolkov_telegram_report_app():
    @task()
    def load_data_lenta():
        # Загружаем общие данные Лента новостей
        query_all_data = '''SELECT toDate(time) as day,
                                count(DISTINCT user_id) AS "DAU",
                                count(DISTINCT post_id) AS "Total posts",
                                sum(source = 'organic') as organic,
                                sum(source = 'ads') as ads,
                                sum(action = 'like') as "Total likes",
                                sum(action = 'view') as "Total views", 
                                sum(gender='1') AS Female,
                                sum(gender='0') AS Male,
                                sum(os='Android') AS Android,
                                sum(os='iOS') AS iOS,
                                "Total likes"/"Total views" as CTR
                            FROM simulator_20230920.feed_actions
                            WHERE toDate(time) > today() -31 AND toDate(time) <= today()
                            GROUP BY toDate(time) as day;
                        '''
        all_data_lenta = ph.read_clickhouse(query_all_data, connection=connection)
        return all_data_lenta

    @task()
    def load_data_top30_posts():
        # Топ постов за 30 дней (можно загнать в файл и выбрать ТОП-10) Лента новостей
        query_top_posts = '''SELECT
                post_id AS post_id,
                countIf(action='view') AS views,
                countIf(action='like') AS likes,
                countIf(action='like') / countIf(action='view') AS ctr,
                count(DISTINCT user_id) AS scope
                FROM simulator_20230920.feed_actions
                WHERE toDate(time) > today() -31 AND toDate(time) < today()
                GROUP BY 
                     post_id
                ORDER BY views DESC;'''
        data_top_posts_lenta = ph.read_clickhouse(query_top_posts, connection=connection)
        return data_top_posts_lenta

    @task()
    def load_data_age_group():
        # Возрастная категория Лента новостей
        query_age_group = '''SELECT 
                        category AS category,
                        count(DISTINCT user_id) AS "Возраст по категориям"

                            FROM (
                                SELECT *,
                                      multiIf(age < 18, '0 - 17', age >= 18
                                      and age <= 34, '18-34', age >= 35
                                      and age <= 44, '35-44', age >= 45
                                      and age <= 54, '45-54', '55+') AS category
                                FROM simulator_20230920.feed_actions
                                WHERE toDate(time) > today() -31 AND toDate(time) < today()
                                 ) AS virtual_table
                         GROUP BY
                            category
                         ORDER BY "Возраст по категориям" DESC

                            '''
        data_age_group_lenta = ph.read_clickhouse(query_age_group, connection=connection)
        return data_age_group_lenta

    @task()
    ## APP Лента новостей и сообщений
    def load_data_active_users():
        # Лента новостей и сообщений Активная аудитория
        query_messages_news = '''SELECT toDate(time) as day,
                                        count(DISTINCT user_id) AS "Number of users"
                            FROM
                                  (SELECT *
                                   FROM
                                         (SELECT user_id,
                                                 time
                                          FROM simulator_20230920.feed_actions) t1
                                          JOIN
                                         (SELECT user_id
                                          FROM simulator_20230920.message_actions) t2 using user_id) AS virtual_table
                            WHERE toDate(time) > today() -31 AND toDate(time) <= today()
                            GROUP BY toDate(time) as day
                            ORDER BY "Number of users" DESC
                            '''

        data_messages_news = ph.read_clickhouse(query_messages_news, connection=connection)
        data_messages_news = data_messages_news.sort_values(by='day', ascending=True).reset_index(drop=True)
        return data_messages_news

    @task()
    def load_data_only_users_lenta_news():
        # Лента новостей и сообщений. Пользователи только ленты новостей
        query_users_only_lenta = '''SELECT toDate(time) as day,
                                              count(DISTINCT user_id) AS "Number of users news feed"
                                       FROM
                                              (SELECT user_id,
                                                      time
                                               FROM simulator_20230920.feed_actions
                                               WHERE user_id NOT IN
                                                    (SELECT user_id
                                                     FROM simulator_20230920.message_actions)) AS virtual_table
                                        WHERE toDate(time) > today() -31 AND toDate(time) <= today()
                                        GROUP BY toDate(time) as day
                                        ORDER BY "Number of users news feed" DESC
                                '''

        data_users_only_lenta = ph.read_clickhouse(query_users_only_lenta, connection=connection)
        data_users_only_lenta = data_users_only_lenta.sort_values(by='day', ascending=True).reset_index(drop=True)
        return data_users_only_lenta

    @task
    def load_data_only_user_messages():
        # Лента новостей и сообщений. Пользователи только сообщений
        query_users_only_messages = '''SELECT toDate(time) as day,
                                              count(DISTINCT user_id) AS "Number of users messages"
                                       FROM
                                              (SELECT *
                                               FROM
                                                     (SELECT user_id
                                                      FROM simulator_20230920.message_actions) t1
                                               LEFT JOIN
                                                     (SELECT user_id,
                                                             time
                                                      FROM simulator_20230920.feed_actions) t2 using user_id) AS virtual_table
                                        WHERE toDate(time) > today() -31 AND toDate(time) <= today()
                                        GROUP BY toDate(time) as day
                                        ORDER BY "Number of users messages" DESC;
                                        '''

        data_users_only_messages = ph.read_clickhouse(query_users_only_messages, connection=connection)
        data_users_only_messages = data_users_only_messages.sort_values(by='day', ascending=True).reset_index(drop=True)
        return data_users_only_messages

    @task
    def load_data_send_reciever_messages():
        # Лента новостей и сообщений. Количество отправленных сообщений и полученных сообщений
        query_send_messages = '''SELECT toDate(time) as day,
                                        count(DISTINCT user_id) AS "Send messages",
                                        count(DISTINCT receiver_id) AS "Reciever messages"
                                 FROM simulator_20230920.message_actions
                                 WHERE toDate(time) > today() -31 AND toDate(time) <= today()
                                 GROUP BY toDate(time) as day
                                 ORDER BY "Send messages" DESC;
                              '''
        data_send_messages = ph.read_clickhouse(query_send_messages, connection=connection)
        data_send_messages = data_send_messages.sort_values(by='day', ascending=True).reset_index(drop=True)

        return data_send_messages

    @task
    def merge_only_lenta_data_only_messages(data_users_only_lenta, data_users_only_messages):
        df_numbers_only_lenta_messages = data_users_only_lenta.merge(data_users_only_messages, on='day', how='left')
        return df_numbers_only_lenta_messages

    ## Вывод значений

    @task
    def send_messages_telegram(data_messages_news, all_data_lenta, data_send_messages, chat_id):
        msg = '''
                📊Events of service: 
                ✔ Users APP
                Today - {}
                Yesterday - {}
                3 days ago - {}
                7 days ago - {}
                10 days ago - {}

                ✔ News feed           (views / likes)
                Today   -         {} / {}
                Yesterday -    {} / {}
                3 days ago -   {} / {}
                7 days ago -   {} / {}
                10 days ago - {} / {}

                ✔ Message service (sent messages / reciever messages):
                Today - {} / {}
                Yesterday - {} / {}    
                3 days ago - {} / {}
                7 days ago - {} / {}
                10 days ago - {} / {}
                ''' \
            .format('{0:,}'.format(data_messages_news.iloc[30, 1]).replace(',', ' '),
                    '{0:,}'.format(data_messages_news.iloc[29, 1]).replace(',', ' '),
                    '{0:,}'.format(data_messages_news.iloc[27, 1]).replace(',', ' '),
                    '{0:,}'.format(data_messages_news.iloc[23, 1]).replace(',', ' '),
                    '{0:,}'.format(data_messages_news.iloc[20, 1]).replace(',', ' '),
                    '{0:,}'.format(all_data_lenta.iloc[30, 6]).replace(',', ' '),
                    '{0:,}'.format(all_data_lenta.iloc[30, 5]).replace(',', ' '),
                    '{0:,}'.format(all_data_lenta.iloc[29, 6]).replace(',', ' '),
                    '{0:,}'.format(all_data_lenta.iloc[29, 5]).replace(',', ' '),
                    '{0:,}'.format(all_data_lenta.iloc[27, 6]).replace(',', ' '),
                    '{0:,}'.format(all_data_lenta.iloc[27, 5]).replace(',', ' '),
                    '{0:,}'.format(all_data_lenta.iloc[23, 6]).replace(',', ' '),
                    '{0:,}'.format(all_data_lenta.iloc[23, 5]).replace(',', ' '),
                    '{0:,}'.format(all_data_lenta.iloc[20, 6]).replace(',', ' '),
                    '{0:,}'.format(all_data_lenta.iloc[20, 5]).replace(',', ' '),
                    '{0:,}'.format(data_send_messages.iloc[30, 1]).replace(',', ' '),
                    '{0:,}'.format(data_send_messages.iloc[30, 2]).replace(',', ' '),
                    '{0:,}'.format(data_send_messages.iloc[29, 1]).replace(',', ' '),
                    '{0:,}'.format(data_send_messages.iloc[29, 2]).replace(',', ' '),
                    '{0:,}'.format(data_send_messages.iloc[27, 1]).replace(',', ' '),
                    '{0:,}'.format(data_send_messages.iloc[27, 2]).replace(',', ' '),
                    '{0:,}'.format(data_send_messages.iloc[23, 1]).replace(',', ' '),
                    '{0:,}'.format(data_send_messages.iloc[23, 2]).replace(',', ' '),
                    '{0:,}'.format(data_send_messages.iloc[20, 1]).replace(',', ' '),
                    '{0:,}'.format(data_send_messages.iloc[20, 2]).replace(',', ' '))
        bot.sendMessage(chat_id=chat_id, text=msg)

    ## Файл

    # Top posts in 30 days

    @task
    def send_file_telegram(data_top_posts_lenta, chat_id):
        file_object = io.StringIO()
        data_top_posts_lenta.to_csv(file_object, sep=';')
        file_object.name = 'Top posts in 30 days.csv'
        file_object.seek(0)
        bot.sendDocument(chat_id=chat_id, document=file_object)

    ### Графики

    # Gender, OS за 30 дней

    @task
    def send_graph_gender_os(all_data_lenta, chat_id):
        colors = sns.color_palette('pastel')[0:5]
        plt.subplot(1, 2, 1)
        plt.suptitle('Показатели за последние 30 дней', fontsize=10)
        plt.pie([all_data_lenta['iOS'].sum(), all_data_lenta['Android'].sum()], labels=['iOS', 'Android'],
                colors=colors, autopct='%.0f%%')
        plt.title("Proportion of devices")
        plt.subplot(1, 2, 2)
        plt.pie([all_data_lenta['Female'].sum(), all_data_lenta['Male'].sum()], labels=['Female', 'Male'],
                colors=colors, autopct='%.0f%%')
        plt.title("Proportion of sexes")
        plt.tight_layout()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Status.png'
        plt.show()
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    # CTR and Total post
    @task
    def send_graph_crt_total_post(all_data_lenta, chat_id):
        fig, axes = plt.subplots(figsize=(10, 9))
        pic1 = sns.lineplot(ax=axes, data=all_data_lenta, x='day', y='CTR')
        # test.set_xticklabels(all_data_lenta['day'],rotation=90, horizontalalignment='right')
        # xticks = axes.get_xticks(all_data_lenta['day'])
        pic1.set_xticklabels([pd.to_datetime(tm, unit='ms').strftime('%Y-%m-%d') for tm in all_data_lenta['day']],
                             rotation=90)
        plt.xticks(all_data_lenta['day'], rotation=90)


        axes.set_title('CTR in time ')
        axes.grid()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Status.png'
        plt.show()
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        fig, axes = plt.subplots(figsize=(10, 9))
        pic2 = sns.lineplot(ax=axes, data=all_data_lenta, x='day', y='Total posts')
        pic2.set_xticklabels(all_data_lenta['day'], rotation=90, horizontalalignment='right')
        pic2.set_xticklabels([pd.to_datetime(tm, unit='ms').strftime('%Y-%m-%d') for tm in all_data_lenta['day']],
                             rotation=90)
        plt.xticks(all_data_lenta['day'], rotation=90)

        axes.set_title('Total posts in time')
        axes.grid()


        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Status.png'
        plt.show()
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    # white, dark, whitegrid, darkgrid, ticks

    @task
    def send_graph_traffic_age_category(all_data_lenta, data_age_group_lenta, chat_id):
        # Organic and ads. Age by category
        color_map = sns.color_palette("Pastel2")
        fig, ax = plt.subplots(1, 2, figsize=(18, 7))

        sns.barplot(ax=ax[0], x="category", y="Возраст по категориям", data=data_age_group_lenta)
        ax[0].set_title('Age by category')
        # plt.xticks(data_age_group_lenta['category'],rotation=90)

        # fig, ax = plt.subplots(1,2, figsize=(18, 8))
        # sns.lineplot(ax = ax, data = all_data_lenta, x = 'day', y = 'organic')
        # sns.lineplot(ax = ax, data = all_data_lenta, x = 'day', y = 'ads')
        sns.lineplot(ax=ax[1], data=all_data_lenta, x='day', y='organic', label='organic')
        sns.lineplot(ax=ax[1], data=all_data_lenta, x='day', y='ads', label='ads')

        # plt.stackplot(all_data_lenta.day, all_data_lenta.organic, all_data_lenta.ads,
        # labels=['ads','organic'],
        # colors=color_map)
        # sns.set(style="dark")

        ax[1].set_title('Organic and ads')
        # plt.legend(loc='upper left')
        ax[1].grid()
        plt.xticks(all_data_lenta['day'], rotation=90)

        plt.show()
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Status.png'

        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    @task
    def send_graph_only_members_lenta_messages(df_numbers_only_lenta_messages, chat_id):
        # Only members lenta and messages
        color_map = sns.color_palette("Pastel2")
        fig, ax = plt.subplots(figsize=(18, 9))
        sns.lineplot(ax=ax, data=df_numbers_only_lenta_messages, x='day', y='Number of users news feed',
                     label='Number of users news feed')
        sns.lineplot(ax=ax, data=df_numbers_only_lenta_messages, x='day', y='Number of users messages',
                     label='Number of users messages')
        # plt.stackplot(df_numbers_only_lenta_messages.day, df_numbers_only_lenta_messages['Number of users messages'], df_numbers_only_lenta_messages['Number of users news feed'],
        # labels=['Number of users messages','Number of users news feed'],
        # colors=color_map)
        plt.xticks(df_numbers_only_lenta_messages['day'], rotation=90)
        ax.set_title('Only members lenta and messages')
        ax.legend()
        plt.legend(loc='upper left')
        ax.grid()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Status.png'

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        plt.show()
        plt.close()

    all_data_lenta = load_data_lenta()
    data_top_posts_lenta = load_data_top30_posts()
    data_age_group_lenta = load_data_age_group()
    data_messages_news = load_data_active_users()
    data_users_only_lenta = load_data_only_users_lenta_news()
    data_users_only_messages = load_data_only_user_messages()
    data_send_messages = load_data_send_reciever_messages()
    df_numbers_only_lenta_messages = merge_only_lenta_data_only_messages(data_users_only_lenta,
                                                                         data_users_only_messages)

    send_messages_telegram(data_messages_news, all_data_lenta, data_send_messages, chat_id)
    send_file_telegram(data_top_posts_lenta, chat_id)
    send_graph_gender_os(all_data_lenta, chat_id)
    send_graph_crt_total_post(all_data_lenta, chat_id)
    send_graph_traffic_age_category(all_data_lenta, data_age_group_lenta, chat_id)
    send_graph_only_members_lenta_messages(df_numbers_only_lenta_messages, chat_id)


korolkov_telegram_report_app = korolkov_telegram_report_app()
