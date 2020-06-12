"""
Command-line management module.
"""
import sys
import argparse
import json
import logging
import os_benchmark
from os_benchmark import utils, benchmarks, errors
from os_benchmark.drivers import errors as driver_errors

ACTIONS = (
    'help',
    'create-bucket',
    'list-buckets',
    'delete-bucket',
    'list-objects',
    'upload',
    'delete-object',
    'clean-bucket',
    'clean',
    'time-upload',
    'time-download',
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
        '-v', '--verbosity',
        default=0, required=False, type=int,
        choices=(0, 1, 2),
        help="Verbosity level; 0=minimal output, 1=normal output 2=verbose output",
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
        for action in ACTIONS:
            action_subparsers[action] = self.subparsers.add_parser(action)
        self.main_args = self.parser.parse_known_args()[0]
        self.subparser = action_subparsers[self.main_args.action]
        self.action = self.main_args.action.replace('-', '_')
        # Logs
        verbosity = 30 - (self.main_args.verbosity * 10)
        self.logger = logging.getLogger('osb')
        console_handler = logging.StreamHandler()
        self.logger.addHandler(console_handler)
        self.logger.setLevel(verbosity)
        # Get config
        if self.main_args.config_raw:
            config = json.loads(self.main_args.config_raw)
        else:
            config = utils.get_driver_config(
                config_name=self.main_args.config_name,
                config_file=self.main_args.config_file,
            )
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
        print(bucket)
        return bucket

    def delete_bucket(self):
        self.subparser.add_argument('bucket_id')
        parsed_args = self.parser.parse_known_args()[0]

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
        content_group.add_argument('--content', type=argparse.FileType('r'), required=False)
        content_group.add_argument('--content-size', type=int, required=False)
        content_group.add_argument('--', '--from-stdin', default=False, action='store_true', dest='from_stdin')
        parsed_args = self.parser.parse_known_args()[0]

        name = parsed_args.name or utils.get_random_name()
        if parsed_args.from_stdin:
            content = sys.stdin
        elif parsed_args.content is not None:
            content = open(parsed_args.content)
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
        )
        return obj

    def list_objects(self):
        self.subparser.add_argument('bucket_id')
        parsed_args = self.parser.parse_known_args()[0]
        try:
            objects = self.driver.list_objects(
                bucket_id=parsed_args.bucket_id,
            )
        except driver_errors.DriverBucketUnfoundError as err:
            self.logger.warning(err.args[0])
            return
        for obj in objects:
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
            input("Press [ENTER] to continue")
        self.driver.clean_bucket(
            bucket_id=parsed_args.bucket_id,
        )

    def clean(self):
        parsed_args = self.parser.parse_known_args()[0]

        if not self.main_args.noinput:
            print("You are going to clean entirely this object storage.")
            input("Press [ENTER] to continue")
        self.driver.clean()

    def time_upload(self):
        self.subparser.add_argument('--storage-class', required=False)
        self.subparser.add_argument('--object-size', type=int, required=True)
        self.subparser.add_argument('--object-number', type=int, required=True)
        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmarks.UploadBenchmark(self.driver)
        benchmark.set_params(
            storage_class=parsed_args.storage_class,
            object_size=parsed_args.object_size,
            object_number=parsed_args.object_number,
        )
        benchmark.setup()
        benchmark.run()
        benchmark.tear_down()
        stats = benchmark.make_stats()
        self.print_stats(stats)

    def time_download(self):
        self.subparser.add_argument('--storage-class', required=False)
        self.subparser.add_argument('--object-size', type=int, required=False)
        self.subparser.add_argument('--object-number', type=int, required=False)
        parsed_args = self.parser.parse_known_args()[0]

        benchmark = benchmarks.DownloadBenchmark(self.driver)
        benchmark.set_params(
            storage_class=parsed_args.storage_class,
            object_size=parsed_args.object_size,
            object_number=parsed_args.object_number,
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
    # Run
    controller = Controller()
    controller.run()


if __name__ == '__main__':
    try:
        main()
    except errors.OsbError as err:
        print(err.args[0])
        sys.exit(1)
    sys.exit(0)
