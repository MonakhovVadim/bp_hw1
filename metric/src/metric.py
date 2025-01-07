import logging
import pika
import json
from pathlib import Path
import pandas as pd

# Для защиты от опечаток и более универсального кода
NAME_Y_TRUE = "y_true"
NAME_Y_PRED = "y_pred"

try:

    # Создаем файл лога, если он отсутствует
    log_dir = Path.cwd() / "logs"
    if not log_dir.exists():
        log_dir.mkdir()

    log = log_dir / "metric_log.csv"
    if not log.exists():
        with log.open("w") as f:
            f.write(f"id,{NAME_Y_TRUE},{NAME_Y_PRED},absolute_error\n")

    # Создаем датафрейм для сбора информации о сообщениях под идентичным
    # id, поэтому будем использовать поле в качестве индекса
    messages = pd.DataFrame(columns=["id", NAME_Y_TRUE, NAME_Y_PRED])
    messages.set_index("id", inplace=True)

    # Создаём подключение к серверу на локальном хосте
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel = connection.channel()

    # Объявляем очередь y_true
    channel.queue_declare(queue=NAME_Y_TRUE)
    # Объявляем очередь y_pred
    channel.queue_declare(queue=NAME_Y_PRED)

    # Создаём функцию callback для обработки данных из очереди
    def callback(ch, method, properties, body):
        message = json.loads(body)
        print(f"Из очереди {method.routing_key} получено сообщение {message}")

        # Добавляем сообщение в сбор информации
        messages.loc[message["id"], method.routing_key] = message["body"]
        row = messages.loc[message["id"]]

        # Если вся информация заполнена, то вычисляем метрику и записываем в файл
        if row.notnull().all():
            absolute_error = abs(row[NAME_Y_TRUE] - row[NAME_Y_PRED])
            with log.open("a") as f:
                values = [
                    message["id"],
                    row[NAME_Y_TRUE],
                    row[NAME_Y_PRED],
                    absolute_error,
                ]
                f.write(f"{','.join(str(i) for i in values)}\n")

    # Извлекаем сообщение из очереди y_true
    channel.basic_consume(
        queue=NAME_Y_TRUE, on_message_callback=callback, auto_ack=True
    )
    # Извлекаем сообщение из очереди y_pred
    channel.basic_consume(
        queue=NAME_Y_PRED, on_message_callback=callback, auto_ack=True
    )

    # Запускаем режим ожидания прихода сообщений
    print("...Ожидание сообщений, для выхода нажмите CTRL+C")
    channel.start_consuming()
except:
    logging.exception("Не удалось подключиться к очереди")
