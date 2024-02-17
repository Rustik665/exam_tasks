# -*- coding: utf-8 -*-
"""
   Реализовать асинхронные методы `perform_operation`, `read_data`, и `send_data`,
   которые будут взаимодействовать с данными и осуществлять асинхронную отправку данных адресатам.
"""
import asyncio
import concurrent.futures
from enum import Enum
from typing import List
from datetime import timedelta
from dataclasses import dataclass

timeout_seconds = timedelta(seconds=15).total_seconds()
QUEUE_MAX_LENGTH = 1000


@dataclass
class Payload:
    data_binary: bytes


@dataclass
class Address:
    ip_address: str
    port: int


@dataclass
class Event:
    recipients: List[Address]
    payload: Payload


class Result(Enum):
    Accepted = 1
    Rejected = 2


RECIPIENTS = [
    Address('192.168.0.1', 21),
    Address('192.168.0.2', 21),
    Address('192.168.0.3', 21),
    Address('192.168.0.4', 21),
    Address('192.168.0.5', 21),
]


def _get_data_binary() -> bytes:
    """ Метод-заглушка получения данных """
    return b'test_payload'


async def read_data() -> Event:
    # Метод для чтения порции данных
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(_get_data_binary)
            concurrent.futures.as_completed([future])
            return Event(RECIPIENTS, Payload(future.result()))
    except:
        return Event(RECIPIENTS, Payload(b''))


def _send_data_binary(dest: Address, payload: Payload) -> Result:
    """ Метод-заглушка отправки данных """
    return Result.Rejected if dest.ip_address == '192.168.0.1' else Result.Accepted


async def send_data(dest: Address, payload: Payload) -> Result:
    # Метод для рассылки данных
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(_send_data_binary, dest, payload)
            concurrent.futures.as_completed([future])
            return future.result()
    except:
        return Result.Rejected


async def _recipient_worker(queue: asyncio.Queue, recipient: Address) -> None:
    """ Работа с одной пачкой данных из очереди по адресату """
    while True:
        payload = await queue.get()
        result = await send_data(recipient, payload)

        # Если не удалось отправить - пробуем еще раз
        if result == Result.Rejected:
            await send_data(recipient, payload)


async def perform_operation() -> None:

    event_queue_by_recipient = dict()
    event = await read_data()

    def create_workers(recipients: List[Address]):
        """
        Для каждого адресата создается очередь для отправки данных в правильном порядке
        и с возможностью повтора
        """
        for r in recipients:
            q = asyncio.Queue(QUEUE_MAX_LENGTH)
            asyncio.create_task(_recipient_worker(q, r))
            event_queue_by_recipient[r.ip_address] = q

    while True:

        # Для новоприбывших запускается воркер работы с очередью отправки для каждого адресата
        new_recipients = [r for r in event.recipients if r.ip_address not in event_queue_by_recipient]
        if new_recipients:
            create_workers(new_recipients)

        # Кладем данные в очередь по адресату
        for recipient in event.recipients:
            recipient_queue = event_queue_by_recipient[recipient.ip_address]
            try:
                recipient_queue.put_nowait(event.payload)
            except asyncio.QueueFull:
                # Не влезло, придется дропнуть эту порцию данных
                print(f'Превышена очередь отправки по адресату: {recipient}, данные отброшены.')

        # Получение новой порции данных
        await asyncio.sleep(1)
        event = await read_data()


# Запуск асинхронного кода
asyncio.run(perform_operation())
