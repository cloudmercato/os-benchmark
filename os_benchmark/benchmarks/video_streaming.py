import asyncio
try:
    import aiohttp
except ImportError:
    aiohttp = None
from os_benchmark import utils, errors
from . import base


class VideoStreamingBenchmark(base.BaseSetupObjectsBenchmark):
    """"""
    def run(self, **kwargs):
        self.sleep(self.params['warmup_sleep'])

        async def download(session, url):
            try:
                async with session.get(url) as response:
                    elapsed, _ = await utils.async_timeit(response.read)
            except aiohttp.client_exceptions.ClientResponseError as err:
                return -1, err
            except aiohttp.client_exceptions.ClientConnectorError as err:
                return -1, err
            except base.ASYNC_TIMEOUT_ERRORS as err:
                return -1, err
            return elapsed, response

        async def download_objets(delay=0):
            await asyncio.sleep(delay)
            connector = aiohttp.TCPConnector(
                limit=self.params.get('max_concurrency'),
            )
            session_kwargs['connector'] = connector
            session = aiohttp.ClientSession(**session_kwargs)
            timings, errs = [], []
            for url in self.urls:
                # self.logger.warning("Download %s", url)
                await asyncio.sleep(self.params['sleep_time'])
                elapsed, response = await download(session, url)
                if elapsed == -1:
                    errors.append(response)
                else:
                    timings.append(elapsed)
            await session.close()
            connector.close()
            return timings, errs

        session_kwargs = {
            'raise_for_status': True,
            'read_timeout': self.driver.read_timeout,
            'conn_timeout': self.driver.connect_timeout
        }
        self.timings = []
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)

        def run():
            clients = []
            for i in range(self.params['client_number']):
                # self.logger.warning("Start client #%d", i)
                delay = i * self.params['delay_time']
                clients.append(download_objets(delay))
            tasks = asyncio.gather(*clients)
            results = loop.run_until_complete(tasks)
            for timings, errs in results:
                self.timings.extend(timings)
                self.errors.extend(errs)

        self.total_time = self.timeit(run)[0]
        loop.close()

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
            'delay_time': self.params['delay_time'],
        }

        stats.update(self._make_aggr(self.timings, 'time'))
        stats.update(self._make_aggr(bws, 'bw', decimals=3))

        if error_count:
            for err in self.errors:
                if isinstance(err, aiohttp.client_exceptions.ClientConnectorError):
                    key = 'error_client'
                elif isinstance(err, base.ASYNC_TIMEOUT_ERRORS):
                    key = 'error_timeout'
                else:
                    key = 'error_count_%s' % err.args[1]
                stats.setdefault(key, 0)
                stats[key] += 1
        return stats
