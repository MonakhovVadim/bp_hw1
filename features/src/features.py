import logging
import time
import pika
import numpy as np
import json
from sklearn.datasets import load_diabetes
from datetime import datetime


def json_message(id, body):
    """
    Формирует словарь из полученных аргументов и сериализует его в json

    Args:
        id (any): идентификатор сообщения
        body (any): объект, который необходимо отправить

    Returns:
        str: серилизованный словарь
    """
    message = json.dumps({"id": id, "body": body})
    return message


# Создаём бесконечный цикл для отправки сообщений в очередь
while True:
    try:
        # Загружаем датасет о диабете
        X, y = load_diabetes(return_X_y=True)
        # Формируем случайный индекс строки
        random_row = np.random.randint(0, X.shape[0] - 1)

        # Создаём подключение по адресу rabbitmq:
        connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
        channel = connection.channel()

        # Создаём очередь y_true
        channel.queue_declare(queue="y_true")
        # Создаём очередь features
        channel.queue_declare(queue="features")

        # Формируем id для сообщений этой итерации
        message_id = datetime.timestamp(datetime.now())

        # Публикуем сообщение в очередь y_true
        message = json_message(message_id, y[random_row])
        channel.basic_publish(
            exchange="",
            routing_key="y_true",
            body=message,
        )
        print(f"Сообщение {message}, отправлено в очередь y_true")

        # Публикуем сообщение в очередь features
        message = json_message(message_id, list(X[random_row]))
        channel.basic_publish(
            exchange="",
            routing_key="features",
            body=message,
        )
        print(f"Сообщение {message}, отправлено в очередь features")

        # Закрываем подключение
        connection.close()
    except:
        logging.exception("Не удалось подключиться к очереди")
    finally:
        time.sleep(2)
