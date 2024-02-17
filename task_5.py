# -*- coding: utf-8 -*-
"""
   Реализовать асинхронные методы для получения статуса заявок от двух сервисов.
   Операция `perform_operation` должна асинхронно выполнять параллельные запросы к сервисам
   `get_application_status1` и `get_application_status2`, а также обрабатывать их ответы
   в соответствии с предоставленными правилами.
"""

import asyncio
import uuid
from abc import ABC, abstractmethod
import concurrent.futures
from enum import Enum
from typing import Optional, List, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta

TIMEOUT_SECONDS = timedelta(seconds=15).total_seconds()
MAX_FAILURES_COUNT = 2


class Response(Enum):
    Success = 1
    RetryAfter = 2
    Failure = 3


class ApplicationStatusResponse(Enum):
    Success = 1
    Failure = 2


@dataclass
class ApplicationResponse:
    application_id: str
    status: ApplicationStatusResponse
    description: str
    last_request_time: datetime
    retriesCount: Optional[int]


class ApplicationInterface(ABC):
    """ Интерфейс приложения """

    def __init__(self, _identifier: str):
        self._id = _identifier

    @abstractmethod
    def get_application_status(self) -> Response:
        """ Возвращает статус заявки """


class ApplicationOne(ApplicationInterface):
    """ Приложение 1 """

    def get_application_status(self) -> Response:
        """ Возвращает статус заявки """
        return Response.Success


class ApplicationTwo(ApplicationInterface):
    """ Приложение 2 """

    IS_SUCCESS = True

    def get_application_status(self) -> Response:
        """ Возвращает статус заявки """
        if ApplicationTwo.IS_SUCCESS:
            ApplicationTwo.IS_SUCCESS = False
            return Response.RetryAfter

        return Response.Success


def _get_application_status(application: ApplicationInterface) -> Response:
    # Метод, возвращающий статус заявки
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(application.get_application_status)
            concurrent.futures.as_completed([future])
            return future.result()
    except:
        return Response.Failure

    # Если результат пришел RetryAfter - попытаться еще раз через asyncio.sleep()


async def get_application_status1(identifier: str) -> Response:
    # Метод, возвращающий статус заявки
    return _get_application_status(ApplicationOne(identifier))


async def get_application_status2(identifier: str) -> Response:
    # Метод, возвращающий статус заявки
    return _get_application_status(ApplicationTwo(identifier))


async def _get_status_with_timeout(
        app_1_id: Optional[str] = None, app_2_id: Optional[str] = None
) -> Tuple[List[Response], bool]:
    """ Возврат результатов с флагом "был таймаут" """
    try:
        if app_1_id and app_2_id:
            result_app1, result_app2 = await asyncio.wait_for(asyncio.gather(
                get_application_status1(app_1_id),
                get_application_status2(app_2_id),
            ), TIMEOUT_SECONDS)
            return [result_app1, result_app2], False
        elif app_1_id:
            result_app1 = await asyncio.wait_for(asyncio.gather([
                get_application_status1(app_1_id),
            ]), TIMEOUT_SECONDS)
            return [result_app1], False
        else:
            result_app2 = await asyncio.wait_for(asyncio.gather([
                get_application_status2(app_2_id),
            ]), TIMEOUT_SECONDS)
            return [result_app2], False
    except asyncio.TimeoutError:
        return [Response.Failure, Response.Failure], True


async def perform_operation(identifier: str) -> ApplicationResponse:
    # Делать запросы с указанным таймаутом одновременно (АСИНХРОННО) в 2 разных сервиса

    retries_count = 0
    last_request_time = datetime.now()
    results, timeout_expired = await _get_status_with_timeout(app_1_id=f'identifier_1', app_2_id=f'identifier_2')
    result_app1, result_app2 = results[0], results[0]

    while (result_app1 == Response.RetryAfter or result_app2 == Response.RetryAfter) \
            and retries_count < MAX_FAILURES_COUNT:
        retries_count += 1
        await asyncio.sleep(TIMEOUT_SECONDS)
        last_request_time = datetime.now()

        if result_app1 == Response.RetryAfter and result_app2 == Response.RetryAfter:
            results, timeout_expired = await _get_status_with_timeout(app_1_id=f'identifier_1', app_2_id=f'identifier_2')
            result_app1, result_app2 = results[0], results[0]

        if timeout_expired:
            break

        if result_app1 == Response.RetryAfter:
            results, timeout_expired = await _get_status_with_timeout(app_1_id=f'identifier_1')
            result_app1 = results[0]

        if result_app2 == Response.RetryAfter:
            results, timeout_expired = await _get_status_with_timeout(app_2_id=f'identifier_2')
            result_app2 = results[0]

    both_apps_succeeded = result_app1 == result_app2 == Response.Success
    result_status = ApplicationStatusResponse.Success if both_apps_succeeded else ApplicationStatusResponse.Failure
    result_description = str(result_status)

    return ApplicationResponse(
        identifier,
        result_status,
        result_description,
        last_request_time,
        retries_count
    )

loop = asyncio.get_event_loop()
result = loop.run_until_complete(perform_operation(str(uuid.uuid4())))
print(result)
