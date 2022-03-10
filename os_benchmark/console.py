"""
Command-line management module.
"""
import os
import sys
import argparse
import json
import os_benchmark
from os_benchmark import utils, benchmarks, errors, logger as logger_
from os_benchmark.drivers import errors as driver_errors

ACTIONS = (
    'help',
    'create-bucket',
    'list-buckets',
    'delete-bucket',
    'list-objects',
    'upload',
    'download',
    'delete-object',
    'clean-bucket',
    'clean',
    'time-upload',
    'time-download',
    'time-multi-download',
    'ab',
    'curl',
    'video-streaming',
)
MULTIPART_THREHOLD = 64 * 2**20
MULTIPART_CHUNKSIZE = 8 * 2**20
MAX_CONCURRENCY = os.cpu_count() * 2

def create_parser():
    """Create main parser"""
    parser = argparse.ArgumentParser(
        prog='os-benchmark',
        add_help=False,
    )
    parser.add_argument(
        '--config-file',
        required=False,
        help="Specify a configuration file to use."
    )
    parser.add_argument(
        '--config-raw',
        required=False,
        help="Provide a raw configuration as JSON instead of a stored file.",
    )
    parser.add_argument(
        '--config-name',
        required=False,
        help="Select a driver configuration to use."
    )
    parser.add_argument(
        '-C', '--connect-timeout',
        default=5, required=False, type=float,
        help="The time in seconds till a timeout is considered during TCP connection.",
    )
    parser.add_argument(
        '-R', '--read-timeout',
        default=10, required=False, type=float,
        help="The time in seconds till a timeout is considered durint HTTP read",
    )
    parser.add_argument(
        '-v', '--verbosity',
        default=0, required=False, type=int,
        choices=(0, 1, 2, 3),
        help="Verbosity level; 0=minimal output, 1=normal output 2=verbose output 3=still more",
    )
    parser.add_argument(
        '-i', '--noinput',
        default=False, action='store_true',
        help="Disable any prompt",
    )
    return parser


class Controller:
    """Helper for organise CLI work"""
    def __init__(self):
        self.parser = create_parser()
        self.subparsers = self.parser.add_subparsers(help="Sub-command", dest='action')

        action_subparsers = {}
        main_action = base_num_args = None
        for action in ACTIONS:
            action_subparsers[action] = self.subparsers.add_parser(action)
            if action in sys.argv:
                main_action = action
                base_num_args = sys.argv.index(action) + 1
        self.main_args = self.parser.parse_known_args(sys.argv[1:base_num_args])[0]
        main_action = self.main_args.action or 'help'
        self.subparser = action_subparsers[main_action]
        self.action = main_action.replace('-', '_')
        # Logs
        self.verbosity = 40 - (self.main_args.verbosity * 10)
        self.logger = logger_.logger
        self.logger.setLevel(self.verbosity)
        # Get config
        if self.main_args.config_raw:
            config = json.loads(self.main_args.config_raw)
        else:
            try:
                config = utils.get_driver_config(
                    config_name=self.main_args.config_name,
                    config_file=self.main_args.config_file,
                )
            except errors.ConfigurationError as err:
                self.logger.error(err)
                self.help()
        config['read_timeout'] = self.main_args.read_timeout
        config['connect_timeout'] = self.main_args.connect_timeout
        # Get driver
        self.driver = utils.get_driver(config)

    def run(self):
        func = getattr(self, self.action)
        result = func()
        return result

    def help(self):
        self.parser.print_help()
        self.parser.exit()

    def create_bucket(self):
        self.subparser.add_argument('--name', required=False)
        self.subparser.add_argument('--storage-class', required=False)
        parsed_args = self.parser.parse_known_args()[0]

        name = parsed_args.name or utils.get_random_name()
        bucket = self.driver.create_bucket(
            name=name,
            storage_class=parsed_args.storage_class,
        )
        return bucket

    def delete_bucket(self):
        self.subparser.add_argument('bucket_id')
        self.subparser.add_argument('--delete-files', action='store_true')
        parsed_args = self.parser.parse_known_args()[0]

        if parsed_args.delete_files:
            try:
                self.driver.clean_bucket(bucket_id=parsed_args.bucket_id)
            except driver_errors.DriverBucketUnfoundError:
                return
        self.driver.delete_bucket(
            bucket_id=parsed_args.bucket_id,
        )

    def list_buckets(self):
        parsed_args = self.parser.parse_known_args()[0]
        buckets = self.driver.list_buckets()
        for bucket in buckets:
            print(bucket['id'])

    def upload(self):
        self.subparser.add_argument('--bucket-id')
        self.subparser.add_argument('--storage-class', required=False)
        self.subparser.add_argument('--name', required=False)
        content_group = self.subparser.add_mutually_exclusive_group()
        content_group.add_argument('--content', type=argparse.FileType('rb'), required=False)
        content_group.add_argument('--content-size', type=int, required=False)
        content_group.add_argument('--', '--from-stdin', default=False, action='store_true', dest='from_stdin')
        self.subparser.add_argument('--multipart-threshold', type=int, default=MULTIPART_THREHOLD)
        self.subparser.add_argument('--multipart-chunksize', type=int, default=MULTIPART_CHUNKSIZE)
        parsed_args = self.parser.parse_known_args()[0]

        name = parsed_args.name or utils.get_random_name()
        if parsed_args.from_stdin:
            content = sys.stdin
        elif parsed_args.content is not None:
            content = parsed_args.content
        elif parsed_args.content_size:
            content = utils.get_random_content(parsed_args.content_size)
        else:
            msg = "No input file given."
            raise errors.OsbError(msg)

        obj = self.driver.upload(
            bucket_id=parsed_args.bucket_id,
            storage_class=parsed_args.storage_class,
            name=name,
            content=content,
            multipart_threshold=parsed_args.multipart_threshold,
            multipart_chunksize=parsed_args.multipart_chunksize,
            max_concurrency=parsed_args.max_concurrency,
        )
        return obj

    def download(self):
        self.subparser.add_argument('--bucket-id')
        self.subparser.add_argument('--name')
        parsed_args = self.parser.parse_known_args()[0]
        url = self.driver.get_url(
            bucket_id=parsed_args.bucket_id,
            name=parsed_args.name,
        )
        self.logger.debug('URL: %s', url)
        fd = self.driver.download(url)
        print(fd.read())


    def list_objects(self):
        self.subparser.add_argument('bucket_id')
        self.subparser.add_argument('--url', action='store_true')
        parsed_args = self.parser.parse_known_args()[0]
        try:
            objects = self.driver.list_objects(
                bucket_id=parsed_args.bucket_id,
            )
        except driver_errors.DriverBucketUnfoundError as err:
            self.logger.warning(err.args[0])
            return
        for obj in objects:
            if parsed_args.url:
                url = self.driver.get_url(
                    bucket_id=parsed_args.bucket_id,
                    name=obj
                )
                print(obj, url)
            else:
                print(obj)

    def delete_object(self):
        self.subparser.add_argument('bucket_id')
        self.subparser.add_argument('name')
        parsed_args = self.parser.parse_known_args()[0]

        self.driver.delete_object(
            bucket_id=parsed_args.bucket_id,
            name=parsed_args.name,
        )

    def clean_bucket(self):
        self.subparser.add_argument('bucket_id')
        parsed_args = self.parser.parse_known_args()[0]

        if not self.main_args.noinput:
            print("You are going to clean entirely this bucket.")
            input("Press [ENTER] to continue\n")
        self.driver.clean_bucket(
            bucket_id=parsed_args.bucket_id,
        )

    def clean(self):
        parsed_args = self.parser.parse_known_args()[0]

        if not self.main_args.noinput:
            print("You are going to clean entirely this object storage.")
            input("Press [ENTER] to continue\n")
        self.driver.clean()

    def time_upload(self):
        self.subparser.add_argument('--storage-class', required=False)
        self.subparser.add_argument('--bucket-prefix', required=False)
        self.subparser.add_argument('--object-size', type=int, required=True)
        self.subparser.add_argument('--object-number', type=int, required=True)
        self.subparser.add_argument('--object-prefix', required=False)
        self.subparser.add_argument('--multipart-threshold', type=int, default=MULTIPART_THREHOLD)
        self.subparser.add_argument('--multipart-chunksize', type=int, default=MULTIPART_CHUNKSIZE)
        self.subparser.add_argument('--max-concurrency', type=int, default=MAX_CONCURRENCY)
        self.subparser.add_argument('--keep-objects', action="store_true")
        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmarks.UploadBenchmark(self.driver)
        benchmark.set_params(
            storage_class=parsed_args.storage_class,
            bucket_prefix=parsed_args.bucket_prefix,
            object_size=parsed_args.object_size,
            object_number=parsed_args.object_number,
            object_prefix=parsed_args.object_prefix,
            multipart_threshold=parsed_args.multipart_threshold,
            multipart_chunksize=parsed_args.multipart_chunksize,
            max_concurrency=parsed_args.max_concurrency,
            keep_objects=parsed_args.keep_objects,
        )
        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def time_download(self):
        self.subparser.add_argument('--storage-class', required=False)
        self.subparser.add_argument('--bucket-prefix', required=False)
        self.subparser.add_argument('--object-size', type=int, required=False)
        self.subparser.add_argument('--object-number', type=int, required=False)
        self.subparser.add_argument('--object-prefix', required=False)
        self.subparser.add_argument('--presigned', action="store_true")
        self.subparser.add_argument('--warmup-sleep', type=int, default=0)
        self.subparser.add_argument('--keep-objects', action="store_true")
        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmarks.DownloadBenchmark(self.driver)
        benchmark.set_params(
            storage_class=parsed_args.storage_class,
            bucket_prefix=parsed_args.bucket_prefix,
            object_size=parsed_args.object_size,
            object_number=parsed_args.object_number,
            object_prefix=parsed_args.object_prefix,
            presigned=parsed_args.presigned,
            warmup_sleep=parsed_args.warmup_sleep,
            keep_objects=parsed_args.keep_objects,
        )
        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def time_multi_download(self):
        self.subparser.add_argument('--storage-class', required=False)
        self.subparser.add_argument('--bucket-prefix', required=False)
        self.subparser.add_argument('--object-size', type=int, required=False)
        self.subparser.add_argument('--object-number', type=int, required=False)
        self.subparser.add_argument('--object-prefix', required=False)
        self.subparser.add_argument('--presigned', action="store_true")
        self.subparser.add_argument('--warmup-sleep', type=int, default=0)
        self.subparser.add_argument('--multipart-chunksize', type=int, default=MULTIPART_CHUNKSIZE)
        self.subparser.add_argument('--max-concurrency', type=int, default=MAX_CONCURRENCY)
        self.subparser.add_argument('--keep-objects', action="store_true")
        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmarks.MultiDownloadBenchmark(self.driver)
        benchmark.set_params(
            storage_class=parsed_args.storage_class,
            bucket_prefix=parsed_args.bucket_prefix,
            object_size=parsed_args.object_size,
            object_number=parsed_args.object_number,
            object_prefix=parsed_args.object_prefix,
            presigned=parsed_args.presigned,
            warmup_sleep=parsed_args.warmup_sleep,
            multipart_chunksize=parsed_args.multipart_chunksize,
            max_concurrency=parsed_args.max_concurrency,
            keep_objects=parsed_args.keep_objects,
        )
        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def ab(self):
        self.subparser.add_argument('--storage-class', required=False)
        self.subparser.add_argument('--bucket-prefix', required=False)
        self.subparser.add_argument('--object-size', type=int, required=False)
        self.subparser.add_argument('--object-number', type=int, required=False)
        self.subparser.add_argument('--object-prefix', required=False)
        self.subparser.add_argument('--presigned', action="store_true")
        self.subparser.add_argument('--concurrency', type=int, default=1)
        self.subparser.add_argument('--timelimit', type=int, default=30)
        self.subparser.add_argument('--num-requests', type=int, default=100)
        self.subparser.add_argument('--keep-alive', action="store_true")
        self.subparser.add_argument('--source-address', required=False)
        self.subparser.add_argument('--keep-objects', action="store_true")
        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmarks.AbBenchmark(self.driver)
        benchmark.set_params(
            storage_class=parsed_args.storage_class,
            bucket_prefix=parsed_args.bucket_prefix,
            object_size=parsed_args.object_size,
            object_number=parsed_args.object_number,
            object_prefix=parsed_args.object_prefix,
            presigned=parsed_args.presigned,
            concurrency=parsed_args.concurrency,
            timelimit=parsed_args.timelimit,
            num_requests=parsed_args.num_requests,
            keep_alive=parsed_args.keep_alive,
            source_address=parsed_args.source_address,
            keep_objects=parsed_args.keep_objects,
        )
        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def curl(self):
        self.subparser.add_argument('--storage-class', required=False)
        self.subparser.add_argument('--bucket-prefix', required=False)
        self.subparser.add_argument('--object-size', type=int, required=False)
        self.subparser.add_argument('--object-number', type=int, required=False)
        self.subparser.add_argument('--object-prefix', required=False)
        self.subparser.add_argument('--presigned', action="store_true")
        self.subparser.add_argument('--warmup-sleep', type=int, default=0)
        self.subparser.add_argument('--keep-alive', action="store_true")
        self.subparser.add_argument('--keep-objects', action="store_true")
        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmarks.PycurlbBenchmark(self.driver)
        benchmark.set_params(
            storage_class=parsed_args.storage_class,
            bucket_prefix=parsed_args.bucket_prefix,
            object_size=parsed_args.object_size,
            object_number=parsed_args.object_number,
            object_prefix=parsed_args.object_prefix,
            presigned=parsed_args.presigned,
            warmup_sleep=parsed_args.warmup_sleep,
            keep_alive=parsed_args.keep_alive,
            keep_objects=parsed_args.keep_objects,
        )
        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def video_streaming(self):
        self.subparser.add_argument('--storage-class', required=False)
        self.subparser.add_argument('--bucket-prefix', required=False)
        self.subparser.add_argument('--object-size', type=int, required=False)
        self.subparser.add_argument('--object-number', type=int, required=False)
        self.subparser.add_argument('--object-prefix', required=False)
        self.subparser.add_argument('--presigned', action="store_true")
        self.subparser.add_argument('--warmup-sleep', type=int, default=0)
        self.subparser.add_argument('--sleep-time', type=int, default=5)
        self.subparser.add_argument('--client-number', type=int, default=1)
        self.subparser.add_argument('--delay-time', type=float, default=.25)
        self.subparser.add_argument('--keep-objects', action="store_true")
        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmarks.VideoStreamingBenchmark(self.driver)
        benchmark.set_params(
            storage_class=parsed_args.storage_class,
            bucket_prefix=parsed_args.bucket_prefix,
            object_size=parsed_args.object_size,
            object_number=parsed_args.object_number,
            object_prefix=parsed_args.object_prefix,
            presigned=parsed_args.presigned,
            warmup_sleep=parsed_args.warmup_sleep,
            sleep_time=parsed_args.sleep_time,
            client_number=parsed_args.client_number,
            delay_time=parsed_args.delay_time,
            keep_objects=parsed_args.keep_objects,
        )
        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def print_stats(self, stats):
        template = '%s\t\t%s'
        print(template % ('version', os_benchmark.__version__))
        for key, value in stats.items():
            if isinstance(value, float):
                value = round(value, 10)
                print('%s\t\t%f' % (key, value))
            else:
                print(template % (key, value))


def main():
    """Entry function"""
    try:
        controller = Controller()
        controller.run()
    except KeyboardInterrupt:
        print("Stopped by user")
        sys.exit(2)
    except errors.OsbError as err:
        if logger_.logger.level <= 0:
            raise
        logger_.logger.error(err)
        sys.exit(1)


if __name__ == '__main__':
    main()
    sys.exit(0)
