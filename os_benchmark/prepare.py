from concurrent.futures import ThreadPoolExecutor
from os_benchmark import utils
from os_benchmark.benchmarks import base


def make_parser_args(parser):
    parser.add_argument('--storage-class', required=False)

    parser.add_argument('--bucket-prefix', required=False, type=utils.unescape)
    parser.add_argument('--bucket-suffix', required=False, type=utils.unescape)
    parser.add_argument('--bucket-id', required=False)

    parser.add_argument('--object-size', type=int)
    parser.add_argument('--object-number', type=int)
    parser.add_argument('--object-prefix', required=False)

    parser.add_argument('--clean', action="store_true")

    parser.add_argument('--multipart-threshold', type=int, default=base.MULTIPART_THREHOLD)
    parser.add_argument('--multipart-chunksize', type=int, default=base.MULTIPART_CHUNKSIZE)
    parser.add_argument('--max-concurrency', type=int, default=base.MAX_CONCURRENCY)
    parser.add_argument('--parallel-objects', type=int, default=1)


def run(args, driver):
    bucket_id = args.bucket_id
    if not bucket_id:
        bucket = driver.create_bucket(
            name=utils.get_random_name(),
            storage_class=args.storage_class,
        )
        bucket_id = bucket['id']
    elif bucket_id and args.clean:
        driver.clean_bucket(bucket_id=bucket_id, delete_bucket=False)

    with ThreadPoolExecutor(max_workers=args.parallel_objects) as executor:
        for i in range(args.object_number):
            content = utils.get_random_content(args.object_size)
            obj = executor.submit(
                driver.upload,
                bucket_id=bucket_id,
                storage_class=args.storage_class,
                name=utils.get_random_name(),
                content=content,
                multipart_threshold=args.multipart_threshold,
                multipart_chunksize=args.multipart_chunksize,
                max_concurrency=args.max_concurrency,
            )
    print(bucket_id)
