"""
Command-line management module.
"""
import sys
import argparse
import json
import logging
from os_benchmark import utils, benchmarks, errors

logger = logging.getLogger('osb')

ACTIONS = (
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


class Controller:
    """Helper for organise CLI work"""
    def __init__(self, driver, parser):
        self.driver = driver
        self.parser = parser

    def create_bucket(self):
        self.parser.add_argument('--name', required=False)
        self.parser.add_argument('--storage-class', required=False)
        parsed_args = self.parser.parse_known_args()[0]

        name = parsed_args.name or utils.get_random_name()
        bucket = self.driver.create_bucket(
            name=name,
            storage_class=parsed_args.storage_class,
        )
        print(bucket)
        return bucket

    def delete_bucket(self):
        self.parser.add_argument('bucket_id')
        parsed_args = self.parser.parse_known_args()[0]

        self.driver.delete_bucket(
            bucket_id=parsed_args.bucket_id,
        )

    def list_buckets(self):
        buckets = self.driver.list_buckets()
        for bucket in buckets:
            print(bucket['id'])

    def upload(self):
        self.parser.add_argument('--bucket-id')
        self.parser.add_argument('--storage-class', required=False)
        self.parser.add_argument('--name', required=False)
        content_group = self.parser.add_mutually_exclusive_group()
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
        self.parser.add_argument('bucket_id')
        parsed_args = self.parser.parse_known_args()[0]

        objects = self.driver.list_objects(
            bucket_id=parsed_args.bucket_id,
        )
        for obj in objects:
            print(obj)

    def delete_object(self):
        self.parser.add_argument('bucket_id')
        self.parser.add_argument('name')
        parsed_args = self.parser.parse_known_args()[0]

        self.driver.delete_object(
            bucket_id=parsed_args.bucket_id,
            name=parsed_args.name,
        )

    def clean_bucket(self):
        self.parser.add_argument('bucket_id')
        parsed_args = self.parser.parse_known_args()[0]

        if not parsed_args.noinput:
            print("You are going to clean entirely this bucket.")
            input("Press [ENTER] to continue")
        self.driver.clean_bucket(
            bucket_id=parsed_args.bucket_id,
        )

    def clean(self):
        parsed_args = self.parser.parse_known_args()[0]
        if not parsed_args.noinput:
            print("You are going to clean entirely this object storage.")
            input("Press [ENTER] to continue")
        self.driver.clean()

    def time_upload(self):
        self.parser.add_argument('--storage-class', required=False)
        self.parser.add_argument('--object-size', type=int, required=False)
        self.parser.add_argument('--object-number', type=int, required=False)
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
        for item in stats.items():
            print('%s\t\t%s' % item)

    def time_download(self):
        self.parser.add_argument('--storage-class', required=False)
        self.parser.add_argument('--object-size', type=int, required=False)
        self.parser.add_argument('--object-number', type=int, required=False)
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
        for item in stats.items():
            print('%s\t\t%s' % item)


def main():
    """Entry function"""
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=ACTIONS)
    parser.add_argument(
        '--config-file',
        required=False,
        help="Specify a configuration file to use."
    )
    parser.add_argument(
        '--config-raw',
        required=False,
        help="Provide a raw configuration as JSON insteadd of a stored file.",
    )
    parser.add_argument(
        '--config-name',
        required=False,
        help="Select a driver configuration to use."
    )
    parser.add_argument(
        '-v', '--verbosity',
        default=1, required=False,
        help="Verbosity level; 0=minimal output, 1=normal output 2=verbose output, 3=very verbose output",
    )
    parser.add_argument(
        '-i', '--noinput',
        default=False, action='store_true',
        help="Disable any prompt",
    )
    parsed_args = parser.parse_known_args()[0]
    # Get config
    if parsed_args.config_raw:
        config = json.loads(parsed_args.config_raw)
    else:
        config = utils.get_driver_config(
            config_name=parsed_args.config_name,
            config_file=parsed_args.config_file,
        )
    # Get driver
    driver = utils.get_driver(config)
    # Run
    controller = Controller(driver, parser)
    action = parsed_args.action.replace('-', '_')
    func = getattr(controller, action)
    result = func()


if __name__ == '__main__':
    try:
        main()
    except errors.OsbError as err:
        print(err.args[0])
        sys.exit(1)
    sys.exit(0)
