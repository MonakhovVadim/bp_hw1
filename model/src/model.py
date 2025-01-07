import logging
import pika
import pickle
import numpy as np
import json


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


# Читаем файл с сериализованной моделью
with open("myfile.pkl", "rb") as pkl_file:
    regressor = pickle.load(pkl_file)

try:
    # Создаём подключение по адресу rabbitmq:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel = connection.channel()

    # Объявляем очередь features
    channel.queue_declare(queue="features")
    # Объявляем очередь y_pred
    channel.queue_declare(queue="y_pred")

    # Создаём функцию callback для обработки данных из очереди
    def callback(ch, method, properties, body):
        message = json.loads(body)
        print(f"Получено сообщение {message}")

        features = message["body"]
        pred = regressor.predict(np.array(features).reshape(1, -1))

        message_to_send = json_message(message["id"], pred[0])
        channel.basic_publish(exchange="", routing_key="y_pred", body=message_to_send)
        print(f"Сообщение {message_to_send} отправлено в очередь y_pred")

    # Извлекаем сообщение из очереди features
    # on_message_callback показывает, какую функцию вызвать при получении сообщения
    channel.basic_consume(queue="features", on_message_callback=callback, auto_ack=True)
    print("...Ожидание сообщений, для выхода нажмите CTRL+C")

    # Запускаем режим ожидания прихода сообщений
    channel.start_consuming()
except:
    logging.exception("Не удалось подключиться к очереди")
