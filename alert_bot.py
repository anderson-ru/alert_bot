from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import logging
import os

from airflow.decorators import dag, task
import pandahouse as ph
import telegram

# Настройки логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Конфигурация: токен Telegram бота и ID чата берутся из переменных окружения
CHAT_ID = int(os.getenv("CHAT_ID", 'your_CHAT_ID'))
TOKEN = os.getenv("TELEGRAM_TOKEN", 'your_token_here')
BOT = telegram.Bot(token=TOKEN)

# Подключение к ClickHouse
CONNECTION = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20241220'
}

# Список ключевых метрик для мониторинга
METRICS = ['views', 'likes', 'CTR', 'active_users_feed', 'active_users_mess', 'message']

# Аргументы DAG по умолчанию
DEFAULT_ARGS = {
    'owner': 'r-anderson',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 2, 10),
}

# Интервал запуска DAG — каждые 15 минут
SCHEDULE_INTERVAL = '*/15 * * * *'

@dag(default_args=DEFAULT_ARGS, schedule_interval=SCHEDULE_INTERVAL, catchup=False)
def bot_alert_1():
    @task()
    def extract_df():
        # SQL-запрос: выбираем данные за последние 7 дней для временного интервала, предшествующего текущему
        query = '''
        WITH max_time AS (
            SELECT formatDateTime(max(toStartOfFifteenMinutes(time)), '%H:%M') AS max_time
            FROM simulator_20241220.feed_actions
            WHERE formatDateTime(toStartOfFifteenMinutes(time), '%H:%M') != formatDateTime(toStartOfFifteenMinutes(now()), '%H:%M')
        ),
        feed_data AS (
            SELECT 
                toStartOfFifteenMinutes(time) AS time15, 
                time::date AS date,
                count(DISTINCT user_id) AS active_users_feed,
                countIf(action = 'view') AS views,
                countIf(action = 'like') AS likes,
                likes / views AS CTR
            FROM simulator_20241220.feed_actions
            WHERE time15 >= today() - 7 
              AND formatDateTime(time15, '%H:%M') = (SELECT max_time FROM max_time)
            GROUP BY time15, date
        ),
        message_data AS (
            SELECT 
                toStartOfFifteenMinutes(time) AS time15, 
                time::date AS date,
                count(DISTINCT user_id) AS active_users_mess,
                count(user_id) AS message
            FROM simulator_20241220.message_actions
            WHERE time15 >= today() - 7 
              AND formatDateTime(time15, '%H:%M') = (SELECT max_time FROM max_time)
            GROUP BY time15, date
        )
        SELECT *
        FROM feed_data
        FULL JOIN message_data USING (time15, date)
        ORDER BY time15 DESC;
        '''
        return ph.read_clickhouse(query, connection=CONNECTION)

    def send_alert(df, metric):
        # Извлекаем текущее значение и временную метку
        current_value = df[metric].iloc[0]
        ct = df.time15.iloc[0]

        # Рассчитываем межквартильный размах и границы
        Q1 = df[metric].quantile(0.25)
        Q3 = df[metric].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        # Проверка на выбросы и отправка алерта
        if current_value < lower_bound or current_value > upper_bound:
            deviation = abs(current_value - (lower_bound if current_value < lower_bound else upper_bound)) / (lower_bound if current_value < lower_bound else upper_bound) * 100

            message = f"""
Метрика: {metric}
Текущее значение: {current_value:.2f}
Отклонение: {deviation:.2f}%
IQR: {IQR:.2f}
Нижняя граница: {lower_bound:.2f}
Верхняя граница: {upper_bound:.2f}
            """
            BOT.sendMessage(chat_id=CHAT_ID, text=message, parse_mode='Markdown')

            # Построение графика
            plt.figure(figsize=(12, 8))
            sns.set(style="whitegrid")
            sns.lineplot(data=df, x=df.time15.dt.strftime('%d %b'), y=metric, color='blue', label=metric)
            plt.title(f'{metric} на {ct.strftime("%H:%M")}', fontsize=16, fontweight='bold')
            plt.axhline(Q1, color='black', linestyle=':', label=f'Q1: {Q1:.2f}')
            plt.axhline(Q3, color='purple', linestyle=':', label=f'Q3: {Q3:.2f}')
            plt.axhline(lower_bound, color='red', linestyle='--', label=f'Нижняя граница: {lower_bound:.2f}')
            plt.axhline(upper_bound, color='green', linestyle='--', label=f'Верхняя граница: {upper_bound:.2f}')
            plt.xlabel('Дата')
            plt.ylabel(metric)
            plt.legend()

            # Отправка графика в чат Telegram
            plot_buf = io.BytesIO()
            plt.savefig(plot_buf, bbox_inches='tight')
            plot_buf.seek(0)
            BOT.sendPhoto(chat_id=CHAT_ID, photo=plot_buf)
            plt.close()

    @task()
    def run_alerts(df):
        # Запускаем проверку по всем заданным метрикам
        for metric in METRICS:
            if metric in df.columns:
                logging.info(f"Проверка метрики: {metric}")
                send_alert(df, metric)

    # DAG: получаем данные → проверяем метрики
    df = extract_df()
    run_alerts(df)

bot_alert_1 = bot_alert_1()
