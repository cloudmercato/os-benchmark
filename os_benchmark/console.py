"""
Command-line management module.
"""
import sys
import argparse
import json
from collections import defaultdict

import os_benchmark
from os_benchmark import logger as logger_
from os_benchmark import utils, benchmarks, errors
from os_benchmark.benchmarks import base
from os_benchmark import prepare
from os_benchmark.drivers import errors as driver_errors

ACTIONS = (
    'help',
    'create-bucket',
    'list-buckets',
    'delete-bucket',
    'list-objects',
    'list-objects-versions',
    'list-object-versions',
    'upload',
    'download',
    'delete-object',
    'copy-object',
    'clean-bucket',
    'clean',

    'prepare',

    'time-upload',
    'time-download',
    'time-multi-download',
    'time-copy',
    'ab',
    'curl',
    'video-streaming',
    'ping',
    'tcpping',
    'traceroute',
    'tcptraceroute',
    'test-features',
)


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
        choices=(0, 1, 2, 3, 4),
        help="Verbosity level; 0=minimal output, 1=normal output 2=verbose output 3=still more",
    )
    parser.add_argument(
        '-i', '--noinput',
        default=False, action='store_true',
        help="Disable any prompt",
    )
    parser.add_argument(
        '--enable-monitoring', action="store_true", dest="monitoring_enabled",
    )
    parser.add_argument(
        '--monitoring-interval', type=int, default=5,
    )
    parser.add_argument(
        '--monitoring-probers', action='append'
    )
    parser.add_argument(
        '--monitoring-output', default="/dev/stderr"
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
        self.verbosity = 40 - (min(self.main_args.verbosity, 3) * 10)
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
        self.driver.set_backend_logger(self.main_args.verbosity)

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
        self.subparser.add_argument('--multipart-threshold', type=int, default=base.MULTIPART_THREHOLD)
        self.subparser.add_argument('--multipart-chunksize', type=int, default=base.MULTIPART_CHUNKSIZE)
        self.subparser.add_argument('--max-concurrency', type=int, default=base.MAX_CONCURRENCY)
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
        self.subparser.add_argument('--versions', action='store_true')
        parsed_args = self.parser.parse_known_args()[0]

        if parsed_args.versions:
            names = []
            versions = self.driver.list_objects_versions(
                bucket_id=parsed_args.bucket_id,
            )
            version_per_obj = defaultdict(list)
            for version in versions:
                version_per_obj[version['name']].append(version['id'])
                names.append(version['name'])
            names = list(set(names))
        else:
            try:
                names = self.driver.list_objects(
                    bucket_id=parsed_args.bucket_id,
                )
            except driver_errors.DriverBucketUnfoundError as err:
                self.logger.warning(err.args[0])
                return

        for name in names:
            line = [name]
            if parsed_args.url:
                url = self.driver.get_url(
                    bucket_id=parsed_args.bucket_id,
                    name=name
                )
                line.append(url)
            print("\t".join(line))
            if parsed_args.versions:
                for version in version_per_obj[name]:
                    print("\t" + version)

    def list_object_versions(self):
        self.subparser.add_argument('bucket_id')
        self.subparser.add_argument('name')
        parsed_args = self.parser.parse_known_args()[0]

        versions = self.driver.list_object_versions(
            bucket_id=parsed_args.bucket_id,
            name=parsed_args.name,
        )
        for version in versions:
            print(version)

    def list_objects_versions(self):
        self.subparser.add_argument('bucket_id')
        parsed_args = self.parser.parse_known_args()[0]

        versions = self.driver.list_objects_versions(
            bucket_id=parsed_args.bucket_id,
        )
        for version in versions:
            print(version)

    def delete_object(self):
        self.subparser.add_argument('bucket_id')
        self.subparser.add_argument('name')
        parsed_args = self.parser.parse_known_args()[0]

        self.driver.delete_object(
            bucket_id=parsed_args.bucket_id,
            name=parsed_args.name,
        )

    def copy_object(self):
        self.subparser.add_argument('bucket_id')
        self.subparser.add_argument('name')
        self.subparser.add_argument('dst_bucket_id')
        self.subparser.add_argument('dst_name')
        parsed_args = self.parser.parse_known_args()[0]

        self.driver.copy_object(
            bucket_id=parsed_args.bucket_id,
            name=parsed_args.name,
            dst_bucket_id=parsed_args.dst_bucket_id,
            dst_name=parsed_args.dst_name,
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
        benchmark_class = base.get_benchmark('upload')
        benchmark_class.make_parser_args(self.subparser)

        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmark_class(self.driver)
        benchmark.set_params(**vars(parsed_args))
        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def time_download(self):
        benchmark_class = base.get_benchmark('download')
        benchmark_class.make_parser_args(self.subparser)

        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmark_class(self.driver)
        benchmark.set_params(**vars(parsed_args))
        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def time_multi_download(self):
        benchmark_class = base.get_benchmark('multi_download')
        benchmark_class.make_parser_args(self.subparser)

        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmark_class(self.driver)
        benchmark.set_params(**vars(parsed_args))
        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def time_copy(self):
        benchmark_class = base.get_benchmark('copy')
        benchmark_class.make_parser_args(self.subparser)

        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmark_class(self.driver)
        benchmark.set_params(**vars(parsed_args))
        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def ab(self):
        benchmark_class = base.get_benchmark('ab')
        benchmark_class.make_parser_args(self.subparser)

        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmark_class(self.driver)
        benchmark.set_params(**vars(parsed_args))

        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def curl(self):
        benchmark_class = base.get_benchmark('curl')
        benchmark_class.make_parser_args(self.subparser)

        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmark_class(self.driver)
        benchmark.set_params(**vars(parsed_args))

        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def video_streaming(self):
        benchmark_class = base.get_benchmark('video_streaming')
        benchmark_class.make_parser_args(self.subparser)

        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmark_class(self.driver)
        benchmark.set_params(**vars(parsed_args))

        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def ping(self):
        benchmark_class = base.get_benchmark('ping')
        benchmark_class.make_parser_args(self.subparser)

        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmark_class(self.driver)
        benchmark.set_params(
            storage_class=parsed_args.storage_class,
            bucket_prefix='',
            object_size=parsed_args.object_size,
            object_number=1,
            object_prefix='',
            presigned=False,
            warmup_sleep=parsed_args.warmup_sleep,
            keep_objects=parsed_args.keep_objects,
            bucket_id=parsed_args.bucket_id,
            ttl=parsed_args.ttl,
            timeout=parsed_args.timeout,
            count=parsed_args.count,
            scapy_verbose=parsed_args.scapy_verbose,
        )
        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def tcpping(self):
        benchmark_class = base.get_benchmark('tcpping')
        benchmark_class.make_parser_args(self.subparser)

        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmark_class(self.driver)
        benchmark.set_params(
            storage_class=parsed_args.storage_class,
            bucket_prefix='',
            object_size=parsed_args.object_size,
            object_number=1,
            object_prefix='',
            presigned=False,
            warmup_sleep=parsed_args.warmup_sleep,
            keep_objects=parsed_args.keep_objects,
            bucket_id=parsed_args.bucket_id,
            ttl=parsed_args.ttl,
            timeout=parsed_args.timeout,
            count=parsed_args.count,
            scapy_verbose=parsed_args.scapy_verbose,
        )
        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def traceroute(self):
        benchmark_class = base.get_benchmark('traceroute')
        benchmark_class.make_parser_args(self.subparser)

        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmark_class(self.driver)
        benchmark.set_params(
            storage_class=parsed_args.storage_class,
            bucket_prefix='',
            object_size=parsed_args.object_size,
            object_number=1,
            object_prefix='',
            presigned=False,
            warmup_sleep=parsed_args.warmup_sleep,
            keep_objects=parsed_args.keep_objects,
            bucket_id=parsed_args.bucket_id,
            max_ttl=parsed_args.max_ttl,
            timeout=parsed_args.timeout,
            count=parsed_args.count,
            scapy_verbose=parsed_args.scapy_verbose,
        )
        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def tcptraceroute(self):
        benchmark_class = base.get_benchmark('tcptraceroute')
        benchmark_class.make_parser_args(self.subparser)

        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmark_class(self.driver)
        benchmark.set_params(
            storage_class=parsed_args.storage_class,
            bucket_prefix='',
            object_size=parsed_args.object_size,
            object_number=1,
            object_prefix='',
            presigned=False,
            warmup_sleep=parsed_args.warmup_sleep,
            keep_objects=parsed_args.keep_objects,
            bucket_id=parsed_args.bucket_id,
            max_ttl=parsed_args.max_ttl,
            timeout=parsed_args.timeout,
            count=parsed_args.count,
            scapy_verbose=parsed_args.scapy_verbose,
        )
        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def test_features(self):
        self.subparser.add_argument('--storage-class', required=False)
        parsed_args = self.parser.parse_known_args()[0]

        benchmark_class = base.get_benchmark('features')
        benchmark_class.make_parser_args(self.subparser)

        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmark_class(self.driver)
        benchmark.set_params(
            storage_class=parsed_args.storage_class,
        )
        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def prepare(self):
        prepare.make_parser_args(self.subparser)
        parsed_args = self.parser.parse_known_args()[0]
        prepare.run(parsed_args, self.driver)

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
        if logger_.logger.level <= 10:
            raise
        sys.exit(2)
    except errors.OsbError as err:
        if logger_.logger.level <= 0:
            raise
        logger_.logger.error(err)
        sys.exit(1)


if __name__ == '__main__':
    main()
    sys.exit(0)
