import logging
import concurrent
import asyncio
try:
    import aiohttp
except ImportError:
    aiohttp = None
from os_benchmark import utils, errors
from . import base

logger = logging.getLogger("osb")


async def _download(session, url):
    """
    Download from a URL and catch errors.
    """
    try:
        async with session.get(url) as response:
            elapsed, _ = await utils.async_timeit(response.read)
    except aiohttp.client_exceptions.ServerDisconnectedError as err:
        return -1, err
    except aiohttp.client_exceptions.ClientResponseError as err:
        return -1, err
    except aiohttp.client_exceptions.ClientConnectorError as err:
        return -1, err
    except base.ASYNC_TIMEOUT_ERRORS as err:
        return -1, err
    return elapsed, response


async def _download_objets(
        urls,
        delay=0,
        read_timeout=5,
        connect_timeout=10,
        max_concurrency=1,
        sleep_time=5,
        process_id=None,
        thread_id=None,
    ):
    """
    Download each object defined by ``urls``.
    A sleep time is applied between each request.
    """
    session_kwargs = {
        'raise_for_status': True,
        'read_timeout': read_timeout,
        'conn_timeout': connect_timeout,
    }
    await asyncio.sleep(delay)
    logger.debug("Started client %s-%s", process_id, thread_id)
    connector = aiohttp.TCPConnector(
        limit=max_concurrency,
    )
    session_kwargs['connector'] = connector
    session = aiohttp.ClientSession(**session_kwargs)
    timings, errs = [], []
    for url in urls:
        await asyncio.sleep(sleep_time)
        elapsed, response = await _download(session, url)
        if elapsed == -1:
            errs.append(response)
        else:
            timings.append(elapsed)
    await session.close()
    connector.close()
    logger.debug("End client %s-%s", process_id, thread_id)
    return timings, errs


def _run_process(
        process_id,
        urls,
        client_number,
        delay,
        sleep_time,
        read_timeout,
        connect_timeout,
    ):
    """
    Run a process containing one or several clients.
    """
    clients = []
    timings = []
    errors = []
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    logger.debug("Started process %s", process_id)
    for i in range(client_number):
        delay = i * delay
        clients.append(_download_objets(
            process_id=process_id,
            thread_id=i,
            urls=urls,
            delay=delay,
            read_timeout=read_timeout,
            connect_timeout=connect_timeout,
            max_concurrency=1,
            sleep_time=sleep_time,
        ))
    tasks = asyncio.gather(*clients)
    results = loop.run_until_complete(tasks)
    for _timings, errs in results:
        timings.extend(_timings)
        errors.extend(errs)

    loop.close()
    return timings, errors


class VideoStreamingBenchmark(base.BaseSetupObjectsBenchmark):
    """
    Download sequentially with sleep-time between objects.
    """
    def run(self, **kwargs):
        self.sleep(self.params['warmup_sleep'])
        self.timings = []
        def run():
            self.logger.debug('Starting processs')
            pool = concurrent.futures.ProcessPoolExecutor(
                max_workers=self.params['process_number'],
            )
            futures = []
            for i in range(self.params['process_number']):
                futures.append(pool.submit(
                    _run_process,
                    i,
                    self.urls,
                    self.params['client_number'],
                    self.params['delay_time'],
                    self.params['sleep_time'],
                    self.driver.read_timeout,
                    self.driver.connect_timeout,
                ))
            for future in futures:
                while not future.done():
                    timings, errs = future.result()
                    self.timings.extend(timings)
                    self.errors.extend(errs)
            pool.shutdown()

        self.total_time = self.timeit(run)[0]

    def make_stats(self):
        count = len(self.timings)
        error_count = len(self.errors)
        size = self.params['object_size']
        total_size = count * size
        bws = [(size/t) for t in self.timings]
        stats = {
            'operation': 'video_streaming',
            'ops': count + error_count,
            'bucket_prefix': self.params.get('bucket_prefix'),
            'object_size': size,
            'object_number': self.params['object_number'],
            'object_prefix': self.params.get('object_prefix'),
            'total_size': total_size,
            'total_time': self.total_time,
            'errors': error_count,
            'error_timeout': 0,
            'driver': self.driver.id,
            'read_timeout': self.driver.read_timeout,
            'connect_timeout': self.driver.connect_timeout,
            'presigned': int(self.params['presigned']),
            'warmup_sleep': self.params['warmup_sleep'],
            'sleep_time': int(self.params['sleep_time']),
            'client_number': int(self.params['client_number']),
            'process_number': int(self.params['process_number']),
            'delay_time': self.params['delay_time'],
        }

        stats.update(self._make_aggr(self.timings, 'time'))
        stats.update(self._make_aggr(bws, 'bw', decimals=3))

        if error_count:
            for err in self.errors:
                if self.logger.level <= 20:
                    self.logger.exception(err)
                if isinstance(err, aiohttp.client_exceptions.ClientConnectorError):
                    key = 'error_client'
                elif isinstance(err, aiohttp.client_exceptions.ServerDisconnectedError):
                    key = 'error_server'
                elif isinstance(err, base.ASYNC_TIMEOUT_ERRORS):
                    key = 'error_timeout'
                else:
                    key = 'error_count_%s' % err.args[1]
                stats.setdefault(key, 0)
                stats[key] += 1
        return stats
