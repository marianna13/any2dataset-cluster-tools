from multiprocessing.pool import ThreadPool
from threading import Semaphore
import urllib.request
import io
import math
import json
import time
import pyarrow as pa
import traceback
import sys

import fsspec
import wandb
import time
from collections import Counter
import json
import multiprocessing
import queue
import pyarrow.parquet as pq
import pyarrow.csv as csv_pa
import pyarrow.json as json_pa
import pyarrow as pa
import pandas as pd
import webdataset as wds
import albumentations as A
import cv2
import numpy as np
from enum import Enum
import imghdr
import signal
from typing import *
import os
import os.path
from contextlib import contextmanager
from multiprocessing import get_context
from itertools import islice, chain

try:
    from pyspark.sql import SparkSession
except ImportError:
    pass
import glob
import time
import argparse

from tqdm import tqdm
import logging
import functools

# import ray
# import memray
LOG_FORMAT = "[P%(process)s/%(name)s] %(levelname)s: %(message)s"
key = "af3a0825037ea837b5f6a13d06c4cb626f3914d4"
wandb.login(key=key)


def ensure_executor_logging(f=None, *, level=logging.INFO, format=LOG_FORMAT):
    """
    Decorator to enable standard Python logging from functions that are used in PySpark executors.
    The PySpark workers are forked processes without any Python logging setup.
    This means that standard Python logging messages are lost (even if logging has been set up in the
    PySpark driver script).
    Use this decorator for functions that are used in PySpark executor contexts, to ensure a minimal logging setup.
    """

    def set_up_logging():
        """Set up logging if not already (short version of `logging.basicConfig`)"""
        root = logging.getLogger()
        if len(root.handlers) == 0:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(format))
            root.addHandler(handler)
            if level is not None:
                root.setLevel(level)

    def decorator(f: Callable):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            set_up_logging()
            return f(*args, **kwargs)

        return wrapped

    # Was decorator used without parenthesis or parameterized?
    return decorator(f) if callable(f) else decorator


_INTER_STR_TO_CV2 = {
    "nearest": cv2.INTER_NEAREST,
    "linear": cv2.INTER_LINEAR,
    "bilinear": cv2.INTER_LINEAR,
    "cubic": cv2.INTER_CUBIC,
    "bicubic": cv2.INTER_CUBIC,
    "area": cv2.INTER_AREA,
    "lanczos": cv2.INTER_LANCZOS4,
    "lanczos4": cv2.INTER_LANCZOS4,
}


def spark_session(master_node, num_cores=16, mem_gb=256):
    """Build a spark session"""

    spark = (
        SparkSession.builder.config("spark.submit.deployMode", "client")
        .master(f"spark://{master_node}:7077")
        .config("spark.driver.port", "5678")
        .config("spark.executor.cores", "4")
        .config("spark.task.cpus", "1")
        .config("spark.default.parallelism", str(int(num_cores)))
        # .config("spark.python.worker.reuse", "false")
        # .config("spark.executor.logs.rolling.maxRetainedFiles", "5")
        # .config("spark.executor.logs.rolling.strategy", "size")
        # .config("spark.executor.logs.rolling.maxSize", "1MB")
        # .config("spark.sql.pyspark.jvmStacktrace.enabled", "true")
        # .config("spark.python.profile", "true")
        # .config("spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled", "false")
        # .config("spark.executor.logs.level", "TRACE")
        # .config("spark.sql.pyspark.jvmStacktrace.enabled", "true")
        # .config("spark.driver.blockManager.port", "6678")
        .config("spark.driver.host", master_node)
        .config("spark.driver.bindAddress", master_node)
        .config(
            "spark.executor.memory", "4G"
        )  # make sure to increase this if you're using more cores per executor
        .config("spark.driver.memory", "1G")
        # .config("spark.driver.cores", "1")
        # .config("spark.executor.memoryOverhead", "1G")
        # .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # .config("spark.memory.fraction", "0.6")
        # .config("spark.executor.instances", "2")
        # .config("spark.dynamicAllocation.initialExecutors", "80")
        # .config("spark.dynamicAllocation.maxExecutors", "80")
        .config("spark.task.maxFailures", "1")
        .appName("sparky")
        .getOrCreate()
    )
    # spark.sparkContext.setLogLevel("DEBUG")

    return spark


def retrier(runf, failed_shards, max_shard_retry):
    # retry failed shards max_shard_retry times
    for i in range(max_shard_retry):
        if len(failed_shards) == 0:
            break
        logging.getLogger("retrier").info(
            f"Retrying {len(failed_shards)} shards, try {i+1}"
        )
        failed_shards = runf(failed_shards)
    if len(failed_shards) != 0:
        logging.getLogger("retrier").info(
            f"Retried {max_shard_retry} times, but {len(failed_shards)} shards "
            "still failed. You may restart the same command to retry again."
        )


def multiprocessing_distributor(
    processes_count, downloader, reader, _, max_shard_retry
):
    """Distribute the work to the processes using multiprocessing"""
    ctx = get_context("spawn")
    with ctx.Pool(processes_count, maxtasksperchild=5) as process_pool:

        def run(gen):
            failed_shards = []
            for status, row in tqdm(process_pool.imap_unordered(downloader, gen)):
                if status is False:
                    failed_shards.append(row)
            return failed_shards

        failed_shards = run(reader)

        retrier(run, failed_shards, max_shard_retry)

        process_pool.terminate()
        process_pool.join()
        del process_pool


def process_row(row, failed_shards):
    status, row = row
    if status is False:
        failed_shards.append(row)


try:
    import ray

    # import memray
    @ray.remote(num_cpus=2)
    def ray_download(downloader, shards):
        status, row = downloader(shards)
        return status, row

    def ray_distributor(processes_count, downloader, reader, _, max_shard_retry):  # type: ignore
        # pylint: disable=unused-argument
        ret = []
        count = 0

        # ray.get([ray_download.remote(downloader, task) for task in reader])

        for task in reader:
            # print(f"Task: {task}")
            #     count += 1
            ret.append(ray_download.remote(downloader, task))
        ray.get(ret)

        # unfinished = ret
        # while len(unfinished) > 0:
        #     finished, unfinished = ray.wait(unfinished, num_returns=1)
        #     ray.get(finished[0])
except ImportError:
    pass


def pyspark_distributor(
    processes_count, downloader, reader, subjob_size, max_shard_retry
):
    """Distribute the work to the processes using pyspark"""

    with _spark_session(processes_count) as spark:

        def batcher(iterable, batch_size):
            iterator = iter(iterable)
            for first in iterator:
                yield list(chain([first], islice(iterator, batch_size - 1)))

        def map_partitions(partitionData):
            for row in partitionData:
                yield downloader(row)

        def run(gen):
            failed_shards = []
            for batch in batcher(gen, subjob_size):
                rdd = spark.sparkContext.parallelize(batch, len(batch))
                # print("Initial partition count:"+str(rdd.getNumPartitions()))
                # map_rdd = rdd.map(downloader)
                # map partitions
                # map_rdd = rdd.map(downloader)
                # map_rdd.foreach(lambda row: process_row(row, failed_shards))
                for status, row in rdd.map(downloader).collect():
                    if status is False:
                        failed_shards.append(row)
            return failed_shards

        failed_shards = run(reader)

        retrier(run, failed_shards, max_shard_retry)


@contextmanager
def _spark_session(processes_count: int):
    """Create and close a spark session if none exist"""

    from pyspark.sql import SparkSession  # pylint: disable=import-outside-toplevel
    import pyspark  # pylint: disable=import-outside-toplevel

    spark_major_version = int(pyspark.version.__version__[0])
    if spark_major_version >= 3:
        spark = SparkSession.getActiveSession()
    else:
        spark = pyspark.sql.SparkSession._instantiatedSession  # type: ignore  # pylint: disable=protected-access

    if spark is None:
        print("No pyspark session found, creating a new one!")
        owned = True
        spark = (
            SparkSession.builder.config("spark.driver.memory", "16G")
            .master("local[" + str(processes_count) + "]")
            .appName("spark-stats")
            .getOrCreate()
        )
    else:
        owned = False

    try:
        yield spark
    finally:
        if owned:
            spark.stop()


class ResizeMode(Enum):
    no = 0  # pylint: disable=invalid-name
    keep_ratio = 1  # pylint: disable=invalid-name
    center_crop = 2  # pylint: disable=invalid-name
    border = 3  # pylint: disable=invalid-name
    keep_ratio_largest = 4  # pylint: disable=invalid-name


# thanks https://stackoverflow.com/questions/11130156/suppress-stdout-stderr-print-from-python-functions
class SuppressStdoutStderr:
    """
    A context manager for doing a "deep suppression" of stdout and stderr in
    Python, i.e. will suppress all print, even if the print originates in a
    compiled C/Fortran sub-function.
       This will not suppress raised exceptions, since exceptions are printed
    to stderr just before a script exits, and after the context manager has
    exited (at least, I think that is why it lets exceptions through).

    """

    def __init__(self):
        # Open a pair of null files
        self.null_fds = [os.open(os.devnull, os.O_RDWR) for x in range(2)]
        # Save the actual stdout (1) and stderr (2) file descriptors.
        self.save_fds = [os.dup(1), os.dup(2)]

    def __enter__(self):
        # Assign the null pointers to stdout and stderr.
        os.dup2(self.null_fds[0], 1)
        os.dup2(self.null_fds[1], 2)

    def __exit__(self, *_):
        # Re-assign the real stdout/stderr back to (1) and (2)
        os.dup2(self.save_fds[0], 1)
        os.dup2(self.save_fds[1], 2)
        # Close all file descriptors
        for fd in self.null_fds + self.save_fds:
            os.close(fd)


def inter_str_to_cv2(inter_str):
    inter_str = inter_str.lower()
    if inter_str not in _INTER_STR_TO_CV2:
        raise ValueError(f"Invalid option for interpolation: {inter_str}")
    return _INTER_STR_TO_CV2[inter_str]


class Resizer:
    """
    Resize images
    Expose a __call__ method to be used as a callable object

    Should be used to resize one image at a time

    Options:
        resize_mode: "no", "keep_ratio", "center_crop", "border"
        resize_only_if_bigger: if True, resize only if image is bigger than image_size
        image_size: size of the output image to resize
    """

    def __init__(
        self,
        image_size,
        resize_mode,
        resize_only_if_bigger,
        upscale_interpolation="lanczos",
        downscale_interpolation="area",
        encode_quality=95,
        encode_format="jpg",
        skip_reencode=False,
        disable_all_reencoding=False,
        min_image_size=0,
        max_image_area=float("inf"),
        max_aspect_ratio=float("inf"),
        blurrer=None,
    ):
        if encode_format not in ["jpg", "png", "webp"]:
            raise ValueError(f"Invalid encode format {encode_format}")
        if encode_format == "png":
            if encode_quality < 0 or encode_quality > 9:
                raise ValueError(
                    "For png, encode quality represents compression which"
                    f"must be between 0 and 9, got {encode_quality}"
                )

        self.image_size = image_size
        if isinstance(resize_mode, str):
            if resize_mode not in ResizeMode.__members__:  # pylint: disable=unsupported-membership-test
                raise ValueError(f"Invalid option for resize_mode: {resize_mode}")
            resize_mode = ResizeMode[resize_mode]
        self.resize_mode = resize_mode
        self.resize_only_if_bigger = resize_only_if_bigger
        self.upscale_interpolation = inter_str_to_cv2(upscale_interpolation)
        self.downscale_interpolation = inter_str_to_cv2(downscale_interpolation)
        self.encode_format = encode_format
        cv2_img_quality = None
        if encode_format == "jpg":
            cv2_img_quality = int(cv2.IMWRITE_JPEG_QUALITY)
            self.what_ext = "jpeg"
        elif encode_format == "png":
            cv2_img_quality = int(cv2.IMWRITE_PNG_COMPRESSION)
            self.what_ext = "png"
        elif encode_format == "webp":
            cv2_img_quality = int(cv2.IMWRITE_WEBP_QUALITY)
            self.what_ext = "webp"
        if cv2_img_quality is None:
            raise ValueError(f"Invalid option for encode_format: {encode_format}")
        self.encode_params = [cv2_img_quality, encode_quality]
        self.skip_reencode = skip_reencode
        self.disable_all_reencoding = disable_all_reencoding
        self.min_image_size = min_image_size
        self.max_image_area = max_image_area
        self.max_aspect_ratio = max_aspect_ratio
        self.blurrer = blurrer

    def __call__(self, img_stream, blurring_bbox_list=None):
        """
        input: an image stream, optionally a list of bounding boxes to blur.
        output: img_str, width, height, original_width, original_height, err
        """
        try:
            if self.disable_all_reencoding:
                return img_stream.read(), None, None, None, None, None
            with SuppressStdoutStderr():
                cv2.setNumThreads(1)
                img_stream.seek(0)
                encode_needed = (
                    imghdr.what(img_stream) != self.what_ext
                    if self.skip_reencode
                    else True
                )
                img_stream.seek(0)
                img_buf = np.frombuffer(img_stream.read(), np.uint8)
                img = cv2.imdecode(img_buf, cv2.IMREAD_UNCHANGED)
                # del img_buf
                if img is None:
                    raise ValueError("Image decoding error")
                if len(img.shape) == 3 and img.shape[-1] == 4:
                    # alpha matting with white background
                    alpha = img[:, :, 3, np.newaxis]
                    img = alpha / 255 * img[..., :3] + 255 - alpha
                    img = np.rint(img.clip(min=0, max=255)).astype(np.uint8)
                    encode_needed = True
                original_height, original_width = img.shape[:2]
                # check if image is too small
                if min(original_height, original_width) < self.min_image_size:
                    return None, None, None, None, None, "image too small"
                if original_height * original_width > self.max_image_area:
                    return None, None, None, None, None, "image area too large"
                # check if wrong aspect ratio
                if (
                    max(original_height, original_width)
                    / min(original_height, original_width)
                    > self.max_aspect_ratio
                ):
                    return None, None, None, None, None, "aspect ratio too large"

                # check if resizer was defined during init if needed
                if blurring_bbox_list is not None and self.blurrer is None:
                    return None, None, None, None, None, "blurrer not defined"

                # Flag to check if blurring is still needed.
                maybe_blur_still_needed = True

                # resizing in following conditions
                if self.resize_mode in (ResizeMode.keep_ratio, ResizeMode.center_crop):
                    downscale = min(original_width, original_height) > self.image_size
                    if not self.resize_only_if_bigger or downscale:
                        interpolation = (
                            self.downscale_interpolation
                            if downscale
                            else self.upscale_interpolation
                        )
                        img = A.smallest_max_size(
                            img, self.image_size, interpolation=interpolation
                        )
                        if blurring_bbox_list is not None and self.blurrer is not None:
                            img = self.blurrer(img=img, bbox_list=blurring_bbox_list)
                        if self.resize_mode == ResizeMode.center_crop:
                            img = A.center_crop(img, self.image_size, self.image_size)
                        encode_needed = True
                        maybe_blur_still_needed = False
                elif self.resize_mode in (
                    ResizeMode.border,
                    ResizeMode.keep_ratio_largest,
                ):
                    downscale = max(original_width, original_height) > self.image_size
                    if not self.resize_only_if_bigger or downscale:
                        interpolation = (
                            self.downscale_interpolation
                            if downscale
                            else self.upscale_interpolation
                        )
                        img = A.longest_max_size(
                            img, self.image_size, interpolation=interpolation
                        )
                        if blurring_bbox_list is not None and self.blurrer is not None:
                            img = self.blurrer(img=img, bbox_list=blurring_bbox_list)
                        if self.resize_mode == ResizeMode.border:
                            img = A.pad(
                                img,
                                self.image_size,
                                self.image_size,
                                border_mode=cv2.BORDER_CONSTANT,
                                value=[255, 255, 255],
                            )
                        encode_needed = True
                        maybe_blur_still_needed = False

                # blur parts of the image if needed
                if (
                    maybe_blur_still_needed
                    and blurring_bbox_list is not None
                    and self.blurrer is not None
                ):
                    img = self.blurrer(img=img, bbox_list=blurring_bbox_list)

                height, width = img.shape[:2]
                if encode_needed:
                    img_str = cv2.imencode(
                        f".{self.encode_format}", img, params=self.encode_params
                    )[1].tobytes()
                else:
                    img_str = img_buf.tobytes()
                return img_str, width, height, original_width, original_height, None

        except Exception as err:  # pylint: disable=broad-except
            return None, None, None, None, None, str(err)


class BufferedParquetWriter:
    """Write samples to parquet files incrementally with a buffer"""

    def __init__(self, output_file, schema, buffer_size=100):
        self.buffer_size = buffer_size
        self.schema = schema
        self._initiatlize_buffer()
        fs, output_path = fsspec.core.url_to_fs(output_file)
        self.output_fd = fs.open(output_path, "wb")
        self.parquet_writer = pq.ParquetWriter(self.output_fd, schema)

    def _initiatlize_buffer(self):
        self.current_buffer_size = 0
        self.buffer = {k: [] for k in self.schema.names}

    def _add_sample_to_buffer(self, sample):
        for k in self.schema.names:
            self.buffer[k].append(sample[k])
        self.current_buffer_size += 1

    def write(self, sample):
        if self.current_buffer_size >= self.buffer_size:
            self.flush()
        self._add_sample_to_buffer(sample)

    def flush(self):
        """Write the buffer to disk"""
        if self.current_buffer_size == 0:
            return

        df = pa.Table.from_pydict(self.buffer, self.schema)
        self.parquet_writer.write_table(df)
        self._initiatlize_buffer()

    def close(self):
        self.flush()
        if self.parquet_writer is not None:
            self.parquet_writer.close()
            self.parquet_writer = None
            self.output_fd.close()


class ParquetSampleWriter:
    """ParquetSampleWriter is a image+caption writer to parquet"""

    def __init__(
        self,
        shard_id,
        output_folder,
        save_caption,
        oom_shard_count,
        schema,
        encode_format,
    ):
        self.oom_shard_count = oom_shard_count
        self.encode_format = encode_format
        schema = schema.append(pa.field(encode_format, pa.binary()))
        shard_name = "{shard_id:0{oom_shard_count}d}".format(  # pylint: disable=consider-using-f-string
            shard_id=shard_id, oom_shard_count=oom_shard_count
        )
        output_file = f"{output_folder}/{shard_name}.parquet"
        self.buffered_parquet_writer = BufferedParquetWriter(output_file, schema, 100)
        self.save_caption = save_caption

    def write(self, img_str, key, caption, meta):
        """Keep sample in memory then write to disk when close() is called"""
        if img_str is not None:
            sample = {"key": key, self.encode_format: img_str}
            if self.save_caption:
                sample["txt"] = str(caption) if caption is not None else ""
        else:
            sample = {"key": key, self.encode_format: None}
            if self.save_caption:
                sample["txt"] = None
        sample.update(meta)
        self.buffered_parquet_writer.write(sample)

    def close(self):
        self.buffered_parquet_writer.close()


class WebDatasetSampleWriter:
    """WebDatasetSampleWriter is a image+caption writer to webdataset"""

    def __init__(
        self,
        shard_id,
        output_folder,
        save_caption,
        oom_shard_count,
        schema,
        encode_format,
    ):
        self.oom_shard_count = oom_shard_count
        shard_name = "{shard_id:0{oom_shard_count}d}".format(  # pylint: disable=consider-using-f-string
            shard_id=shard_id, oom_shard_count=oom_shard_count
        )
        self.shard_id = shard_id
        fs, output_path = fsspec.core.url_to_fs(output_folder)
        self.tar_fd = fs.open(f"{output_path}/{shard_name}.tar", "wb")
        self.tarwriter = wds.TarWriter(self.tar_fd)
        self.save_caption = save_caption
        self.buffered_parquet_writer = BufferedParquetWriter(
            output_folder + "/" + shard_name + ".parquet", schema, 100
        )
        self.encode_format = encode_format

    def write(self, img_str, key, caption, meta):
        """write sample to tars"""
        if img_str is not None:
            sample = {"__key__": key, self.encode_format: img_str}
            if self.save_caption:
                sample["txt"] = str(caption) if caption is not None else ""
            # some meta data may not be JSON serializable
            for k, v in meta.items():
                if isinstance(v, np.ndarray):
                    meta[k] = v.tolist()
            sample["json"] = json.dumps(meta, indent=4)
            self.tarwriter.write(sample)
        self.buffered_parquet_writer.write(meta)

    def close(self):
        self.buffered_parquet_writer.close()
        self.tarwriter.close()
        self.tar_fd.close()


class TFRecordSampleWriter:
    """TFRecordSampleWriter is a image+caption writer to TFRecord"""

    def __init__(
        self,
        shard_id,
        output_folder,
        save_caption,
        oom_shard_count,
        schema,
        encode_format,
    ):
        try:
            os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
            import tensorflow_io as _  # pylint: disable=import-outside-toplevel
            from tensorflow.python.lib.io.tf_record import TFRecordWriter  # pylint: disable=import-outside-toplevel
            from tensorflow.python.training.training import (  # pylint: disable=import-outside-toplevel
                BytesList,
                Example,
                Feature,
                Features,
                FloatList,
                Int64List,
            )

            self._BytesList = BytesList  # pylint: disable=invalid-name
            self._Int64List = Int64List  # pylint: disable=invalid-name
            self._FloatList = FloatList  # pylint: disable=invalid-name
            self._Example = Example  # pylint: disable=invalid-name
            self._Features = Features  # pylint: disable=invalid-name
            self._Feature = Feature  # pylint: disable=invalid-name
        except ImportError as e:
            raise ModuleNotFoundError(
                "tfrecords require tensorflow and tensorflow_io to be installed."
                "Run `pip install tensorflow tensorflow_io`."
            ) from e

        self.oom_shard_count = oom_shard_count
        shard_name = "{shard_id:0{oom_shard_count}d}".format(  # pylint: disable=consider-using-f-string
            shard_id=shard_id, oom_shard_count=oom_shard_count
        )
        self.shard_id = shard_id
        self.tf_writer = TFRecordWriter(f"{output_folder}/{shard_name}.tfrecord")
        self.save_caption = save_caption
        self.buffered_parquet_writer = BufferedParquetWriter(
            output_folder + "/" + shard_name + ".parquet", schema, 100
        )
        self.encode_format = encode_format

    def write(self, img_str, key, caption, meta):
        """Write a sample using tfrecord writer"""
        if img_str is not None:
            sample = {
                "key": self._bytes_feature(key.encode()),
                self.encode_format: self._bytes_feature(img_str),
            }
            if self.save_caption:
                sample["txt"] = self._bytes_feature(
                    str(caption) if caption is not None else ""
                )
            for k, v in meta.items():
                sample[k] = self._feature(v)
            tf_example = self._Example(features=self._Features(feature=sample))
            self.tf_writer.write(tf_example.SerializeToString())
        self.buffered_parquet_writer.write(meta)

    def close(self):
        self.buffered_parquet_writer.close()
        self.tf_writer.close()

    def _feature(self, value):
        """Convert to proper feature type"""
        if isinstance(value, list):
            return self._list_feature(value)
        elif isinstance(value, int):
            return self._int64_feature(value)
        elif isinstance(value, float):
            return self._float_feature(value)
        else:
            return self._bytes_feature(value)

    def _bytes_feature(self, value):
        """Returns a bytes_list from a string / byte."""
        if value is None:
            value = ""
        if isinstance(value, str):
            value = value.encode()
        return self._Feature(bytes_list=self._BytesList(value=[value]))

    def _float_feature(self, value):
        """Returns a float_list from a float / double."""
        return self._Feature(float_list=self._FloatList(value=[value]))

    def _int64_feature(self, value):
        """Returns an int64_list from a bool / enum / int / uint."""
        return self._Feature(int64_list=self._Int64List(value=[value]))

    def _list_feature(self, value):
        """Returns an list of int64_list, float_list, bytes_list."""
        if isinstance(value[0], int):
            return self._Feature(int64_list=self._Int64List(value=value))
        elif isinstance(value[0], float):
            return self._Feature(float_list=self._FloatList(value=value))
        else:
            for i, bytes_feature in enumerate(value):
                if bytes_feature is None:
                    value[i] = ""
                if isinstance(bytes_feature, str):
                    value[i] = bytes_feature.encode()
            return self._Feature(bytes_list=self._BytesList(value=value))


class FilesSampleWriter:
    """FilesSampleWriter is a caption+image writer to files"""

    def __init__(
        self,
        shard_id,
        output_folder,
        save_caption,
        oom_shard_count,
        schema,
        encode_format,
    ):
        self.oom_shard_count = oom_shard_count
        shard_name = "{shard_id:0{oom_shard_count}d}".format(  # pylint: disable=consider-using-f-string
            shard_id=shard_id, oom_shard_count=oom_shard_count
        )
        self.shard_id = shard_id
        self.fs, self.subfolder = fsspec.core.url_to_fs(f"{output_folder}/{shard_name}")
        if not self.fs.exists(self.subfolder):
            self.fs.mkdir(self.subfolder)
        self.save_caption = save_caption
        self.buffered_parquet_writer = BufferedParquetWriter(
            output_folder + "/" + shard_name + ".parquet", schema, 100
        )
        self.encode_format = encode_format

    def write(self, img_str, key, caption, meta):
        """Write sample to disk"""
        if img_str is not None:
            filename = f"{self.subfolder}/{key}.{self.encode_format}"
            with self.fs.open(filename, "wb") as f:
                f.write(img_str)
            if self.save_caption:
                caption = str(caption) if caption is not None else ""
                caption_filename = f"{self.subfolder}/{key}.txt"
                with self.fs.open(caption_filename, "w") as f:
                    f.write(str(caption))

            # some meta data may not be JSON serializable
            for k, v in meta.items():
                if isinstance(v, np.ndarray):
                    meta[k] = v.tolist()
            j = json.dumps(meta, indent=4)
            meta_filename = f"{self.subfolder}/{key}.json"
            with self.fs.open(meta_filename, "w") as f:
                f.write(j)
        self.buffered_parquet_writer.write(meta)

    def close(self):
        self.buffered_parquet_writer.close()


class Reader:
    """
    The reader class reads an url list and returns shards
    It provides an iter method
    It provides attributes:
    - column_list: the list of columns to read
    - input_format: the format of the input file
    - url_col: the column name of the url
    - caption_col: the column name of the caption
    - verify_hash_col: the column containing the hash to verify.
    - verify_hash_type: the type of hash to verify.
    - save_additional_columns: the list of additional columns to save
    - number_sample_per_shard: the number of samples per shard
    - done_shards: a set of already done shards
    - start_shard_id: the shard id to begin downloading from
    """

    def __init__(
        self,
        url_list,
        input_format,
        url_col,
        caption_col,
        verify_hash_col,
        verify_hash_type,
        save_additional_columns,
        number_sample_per_shard,
        done_shards,
        tmp_path,
        start_shard_id: int = 0,
        start_file_id: int = 0,
    ) -> None:
        self.input_format = input_format
        self.url_col = url_col
        self.caption_col = caption_col
        self.verify_hash_col = verify_hash_col
        self.verify_hash_type = verify_hash_type
        self.save_additional_columns = save_additional_columns
        self.number_sample_per_shard = number_sample_per_shard
        self.done_shards = done_shards
        self.start_shard_id = start_shard_id

        fs, url_path = fsspec.core.url_to_fs(url_list)
        self.fs = fs
        self.tmp_path = tmp_path
        self.start_file_id = start_file_id

        if fs.isdir(url_path):
            self.input_files = sorted(
                fs.glob(url_path.rstrip("/") + "/*." + input_format)
            )
            if len(self.input_files) == 0:
                raise ValueError(
                    f"No file found at path {url_path} with extension {input_format}"
                )
        else:
            self.input_files = [url_path]

        if self.input_format in ["txt", "txt.gz"]:
            self.column_list = ["url"]
        elif self.input_format in [
            "json",
            "json.gz",
            "jsonl",
            "jsonl.gz",
            "csv",
            "csv.gz",
            "tsv",
            "tsv.gz",
            "parquet",
        ]:
            self.column_list = (
                self.save_additional_columns
                if self.save_additional_columns is not None
                else []
            )
            if self.caption_col is not None:
                self.column_list = self.column_list + ["caption"]
            self.column_list = self.column_list + ["url"]
        else:
            raise ValueError(f"Invalid input format {self.input_format}")

    def _save_to_arrow(self, input_file, start_shard_id):
        """Read the input file and save to arrow files in a temporary directory"""
        if self.input_format in [
            "txt",
            "txt.gz",
            "csv",
            "csv.gz",
            "tsv",
            "tsv.gz",
            "json",
            "json.gz",
            "jsonl",
            "jsonl.gz",
        ]:
            compression = None
            if self.input_format.endswith(".gz"):
                compression = "gzip"
            with self.fs.open(
                input_file, encoding="utf-8", mode="rb", compression=compression
            ) as file:
                if self.input_format in ["txt", "txt.gz"]:
                    df = csv_pa.read_csv(
                        file, read_options=csv_pa.ReadOptions(column_names=["url"])
                    )
                elif self.input_format in ["json", "json.gz"]:
                    df = pa.Table.from_pandas(pd.read_json(file))
                elif self.input_format in ["csv", "csv.gz"]:
                    df = csv_pa.read_csv(file)
                elif self.input_format in ["tsv", "tsv.gz"]:
                    df = csv_pa.read_csv(
                        file, parse_options=csv_pa.ParseOptions(delimiter="\t")
                    )
                elif self.input_format in ["jsonl", "jsonl.gz"]:
                    df = json_pa.read_json(file)
                else:
                    raise ValueError(f"Unknown input format {self.input_format}")
        elif self.input_format == "parquet":
            with self.fs.open(input_file, mode="rb") as file:
                columns_to_read = [self.url_col]
                if self.caption_col is not None:
                    columns_to_read += [self.caption_col]
                if self.save_additional_columns is not None:
                    columns_to_read += self.save_additional_columns
                df = pq.read_table(file, columns=columns_to_read)
        else:
            raise ValueError(f"Unknown input format {self.input_format}")

        column_names = df.column_names
        if self.caption_col is not None:
            column_names = [
                c if c != self.caption_col else "caption" for c in column_names
            ]

        column_names = [c if c != self.url_col else "url" for c in column_names]

        df = df.rename_columns(column_names)

        number_samples = df.num_rows

        number_shards = math.ceil(df.num_rows / self.number_sample_per_shard)
        shards_to_write = [
            (start_shard_id + shard_id, shard_id)
            for shard_id in range(number_shards)
            if start_shard_id + shard_id not in self.done_shards
        ]
        if len(shards_to_write) == 0:
            return [], number_shards

        def write_shard(t):
            full_shard_id, shard_id = t
            begin_shard = shard_id * self.number_sample_per_shard
            end_shard = min(
                number_samples, (1 + shard_id) * self.number_sample_per_shard
            )
            df_shard = df.slice(begin_shard, end_shard - begin_shard).select(
                self.column_list
            )
            tmp_file = self.tmp_path + f"/{full_shard_id}.feather"
            for i in range(10):
                try:
                    fs, tmp_path = fsspec.core.url_to_fs(tmp_file)
                    with fs.open(tmp_path, "wb") as file:
                        with pa.ipc.new_file(file, df_shard.schema) as writer:
                            writer.write_table(df_shard)
                    del df_shard
                    return (full_shard_id, tmp_file)
                except Exception as e:  # pylint: disable=broad-except
                    if i != 9:
                        print("retrying to write to file due to error:", e)
                        time.sleep(1)
                    else:
                        raise e
            # can't reach here
            raise ValueError("Failed to write to file.")

        for i in range(10):
            shards = []
            # thread pool to make it faster to write files to low latency file systems (ie s3, hdfs)
            try:
                with ThreadPool(24) as thread_pool:
                    for shard in thread_pool.imap_unordered(
                        write_shard, shards_to_write
                    ):
                        shards.append(shard)
                break
            except Exception as e:  # pylint: disable=broad-except
                if i != 9:
                    print("retrying whole sharding to write to files due to error:", e)
                    time.sleep(2 * i)
                else:
                    raise e

        del df
        shards.sort(key=lambda k: k[0])

        del shards_to_write

        return shards, number_shards

    def __iter__(self):
        """
        Iterate over shards, yield shards of size number_sample_per_shard or less for the last one
        Each shard is a tuple (shard_id, shard)
        shard is a tuple (sample id, sample)
        sample is a tuple of the columns
        """
        start_shard_id = self.start_shard_id
        print(len(self.input_files))
        for i, input_file in enumerate(self.input_files):
            if i < self.start_file_id and self.start_file_id != 0:
                continue
            print(
                "Sharding file number "
                + str(i + 1)
                + " of "
                + str(len(self.input_files))
                + " called "
                + input_file
            )

            shards, number_shards = self._save_to_arrow(input_file, start_shard_id)
            print("File sharded in " + str(len(shards)) + " shards")
            print(
                "Downloading starting now, check your bandwidth speed (with bwm-ng)"
                "your cpu (with htop), and your disk usage (with iotop)!"
            )

            for shard_id, arrow_file in shards:
                yield (
                    shard_id,
                    arrow_file,
                )
            start_shard_id += number_shards


class Logger:
    """logger which logs when number of calls reaches a value or a time interval has passed"""

    def __init__(self, min_interval=0):
        """Log only every if min_interval (seconds) have elapsed since last log"""
        # wait for all processes to return
        self.processes_returned = 0
        # min time (in seconds) before logging a new table (avoids too many logs)
        self.min_interval = min_interval
        self.last = time.perf_counter()
        # keep track of whether we logged the last call
        self.last_call_logged = False
        self.last_args = None
        self.last_kwargs = None

    def __call__(self, *args, **kwargs):
        self.processes_returned += 1
        if time.perf_counter() - self.last > self.min_interval:
            self.do_log(*args, **kwargs)
            self.last = time.perf_counter()
            self.last_call_logged = True
        else:
            self.last_call_logged = False
            self.last_args = args
            self.last_kwargs = kwargs

    def do_log(self, *args, **kwargs):
        raise NotImplementedError()

    def sync(self):
        """Ensure last call is logged"""
        if not self.last_call_logged and self.last_args is not None:
            self.do_log(*self.last_args, **self.last_kwargs)
            # reset for next file
            self.processes_returned = 0


class SpeedLogger(Logger):
    """Log performance metrics"""

    def __init__(self, prefix, enable_wandb, **logger_args):
        super().__init__(**logger_args)
        self.prefix = prefix
        self.start_time = float("+inf")
        self.end_time = float("-inf")
        self.count = 0
        self.success = 0
        self.failed_to_download = 0
        self.failed_to_resize = 0
        self.enable_wandb = enable_wandb

    def __call__(
        self, count, success, failed_to_download, failed_to_resize, start_time, end_time
    ):  # pylint: disable=arguments-differ
        self.count += count
        self.success += success
        self.failed_to_download += failed_to_download
        self.failed_to_resize += failed_to_resize
        self.start_time = min(start_time, self.start_time)
        self.end_time = max(end_time, self.end_time)
        super().__call__(
            self.count,
            self.success,
            self.failed_to_download,
            self.failed_to_resize,
            self.start_time,
            self.end_time,
        )

    def do_log(
        self, count, success, failed_to_download, failed_to_resize, start_time, end_time
    ):  # pylint: disable=arguments-differ
        duration = end_time - start_time
        img_per_sec = count / duration
        success_ratio = 1.0 * success / count
        failed_to_download_ratio = 1.0 * failed_to_download / count
        failed_to_resize_ratio = 1.0 * failed_to_resize / count

        print(
            " - ".join(
                [
                    f"{self.prefix:<7}",
                    f"success: {success_ratio:.3f}",
                    f"failed to download: {failed_to_download_ratio:.3f}",
                    f"failed to resize: {failed_to_resize_ratio:.3f}",
                    f"images per sec: {img_per_sec:.0f}",
                    f"count: {count}",
                ]
            )
        )

        if self.enable_wandb:
            wandb.log(
                {
                    f"{self.prefix}/img_per_sec": img_per_sec,
                    f"{self.prefix}/success": success_ratio,
                    f"{self.prefix}/failed_to_download": failed_to_download_ratio,
                    f"{self.prefix}/failed_to_resize": failed_to_resize_ratio,
                    f"{self.prefix}/count": count,
                }
            )


class StatusTableLogger(Logger):
    """Log status table to W&B, up to `max_status` most frequent items"""

    def __init__(
        self, max_status=100, min_interval=60, enable_wandb=False, **logger_args
    ):
        super().__init__(min_interval=min_interval, **logger_args)
        # avoids too many errors unique to a specific website (SSL certificates, etc)
        self.max_status = max_status
        self.enable_wandb = enable_wandb

    def do_log(self, status_dict, count):  # pylint: disable=arguments-differ
        if self.enable_wandb:
            status_table = wandb.Table(
                columns=["status", "frequency", "count"],
                data=[
                    [k, 1.0 * v / count, v]
                    for k, v in status_dict.most_common(self.max_status)
                ],
            )
            wandb.run.log({"status": status_table})


def write_stats(
    output_folder,
    shard_id,
    count,
    successes,
    failed_to_download,
    failed_to_resize,
    start_time,
    end_time,
    status_dict,
    oom_shard_count,
):
    """Write stats to disk"""
    stats = {
        "count": count,
        "successes": successes,
        "failed_to_download": failed_to_download,
        "failed_to_resize": failed_to_resize,
        "duration": end_time - start_time,
        "start_time": start_time,
        "end_time": end_time,
        "status_dict": status_dict.dump(),
    }
    fs, output_path = fsspec.core.url_to_fs(output_folder)
    shard_name = "{shard_id:0{oom_shard_count}d}".format(  # pylint: disable=consider-using-f-string
        shard_id=shard_id, oom_shard_count=oom_shard_count
    )
    json_file = f"{output_path}/{shard_name}_stats.json"
    with fs.open(json_file, "w") as f:
        json.dump(stats, f, indent=4)


# https://docs.python.org/3/library/multiprocessing.html
# logger process that reads stats files regularly, aggregates and send to wandb / print to terminal
class LoggerProcess(multiprocessing.context.SpawnProcess):
    """Logger process that reads stats files regularly, aggregates and send to wandb / print to terminal"""

    def __init__(
        self,
        output_folder,
        enable_wandb,
        wandb_project,
        config_parameters,
        log_interval=5,
    ):
        super().__init__()
        self.log_interval = log_interval
        self.enable_wandb = enable_wandb
        self.output_folder = output_folder
        self.stats_files = set()
        self.wandb_project = wandb_project
        self.done_shards = set()
        self.config_parameters = config_parameters
        ctx = multiprocessing.get_context("spawn")
        self.q = ctx.Queue()

    def run(self):
        """Run logger process"""

        fs, output_path = fsspec.core.url_to_fs(
            self.output_folder, use_listings_cache=False
        )

        if self.enable_wandb:
            self.current_run = wandb.init(
                project=self.wandb_project,
                config=self.config_parameters,
                anonymous="allow",
            )
        else:
            self.current_run = None
        self.total_speed_logger = SpeedLogger("total", enable_wandb=self.enable_wandb)
        self.status_table_logger = StatusTableLogger(enable_wandb=self.enable_wandb)
        last_check = 0
        total_status_dict = CappedCounter()
        while True:
            time.sleep(0.1)
            try:
                self.q.get(False)
                last_one = True
            except queue.Empty as _:
                last_one = False
            if not last_one and time.perf_counter() - last_check < self.log_interval:
                continue

            try:
                # read stats files
                stats_files = fs.glob(output_path + "/*.json")

                # filter out files that have an id smaller that are already done
                stats_files = [
                    f
                    for f in stats_files
                    if int(f.split("/")[-1].split("_")[0]) not in self.done_shards
                ]

                # get new stats files
                new_stats_files = set(stats_files) - self.stats_files
                if len(new_stats_files) == 0:
                    if last_one:
                        self.finish()
                        return

                # read new stats files
                for stats_file in new_stats_files:
                    with fs.open(stats_file, "r") as f:
                        try:
                            stats = json.load(f)
                            SpeedLogger("worker", enable_wandb=self.enable_wandb)(
                                count=stats["count"],
                                success=stats["successes"],
                                failed_to_download=stats["failed_to_download"],
                                failed_to_resize=stats["failed_to_resize"],
                                start_time=stats["start_time"],
                                end_time=stats["end_time"],
                            )
                            self.total_speed_logger(
                                count=stats["count"],
                                success=stats["successes"],
                                failed_to_download=stats["failed_to_download"],
                                failed_to_resize=stats["failed_to_resize"],
                                start_time=stats["start_time"],
                                end_time=stats["end_time"],
                            )
                            status_dict = CappedCounter.load(stats["status_dict"])
                            total_status_dict.update(status_dict)
                            self.status_table_logger(
                                total_status_dict, self.total_speed_logger.count
                            )
                        except Exception as err:  # pylint: disable=broad-except
                            print(f"failed to parse stats file {stats_file}", err)

                    self.stats_files.add(stats_file)
                last_check = time.perf_counter()

                if last_one:
                    self.finish()
                    return
            except Exception as e:  # pylint: disable=broad-except
                traceback.print_exc()
                print("logger error", e)
                self.finish()
                return

    def finish(self):
        """Finish logger process"""
        self.total_speed_logger.sync()
        self.status_table_logger.sync()
        if self.current_run is not None:
            self.current_run.finish()

    def join(self, timeout=None):
        """Stop logger process"""
        self.q.put("stop")
        super().join()
        self.q.close()


class CappedCounter:
    """Maintain a counter with a capping to avoid memory issues"""

    def __init__(self, max_size=10**5):
        self.max_size = max_size
        self.counter = Counter()

    def increment(self, key):
        if len(self.counter) >= self.max_size:
            self._keep_most_frequent()
        self.counter[key] += 1

    def _keep_most_frequent(self):
        self.counter = Counter(dict(self.counter.most_common(int(self.max_size / 2))))

    def most_common(self, k):
        return self.counter.most_common(k)

    def update(self, counter):
        self.counter.update(counter.counter)
        if len(self.counter) >= self.max_size:
            self._keep_most_frequent()

    def dump(self):
        return self.counter

    @classmethod
    def load(cls, d, max_size=10**5):
        c = CappedCounter(max_size)
        c.counter = Counter(d)
        return c


def is_disallowed(headers, user_agent_token, disallowed_header_directives):
    """Check if HTTP headers contain an X-Robots-Tag directive disallowing usage"""
    for values in headers.get_all("X-Robots-Tag", []):
        try:
            uatoken_directives = values.split(":", 1)
            directives = [x.strip().lower() for x in uatoken_directives[-1].split(",")]
            ua_token = (
                uatoken_directives[0].lower() if len(uatoken_directives) == 2 else None
            )
            if (ua_token is None or ua_token == user_agent_token) and any(
                x in disallowed_header_directives for x in directives
            ):
                return True
        except Exception as err:  # pylint: disable=broad-except
            traceback.print_exc()
            print(f"Failed to parse X-Robots-Tag: {values}: {err}")
    return False


def download_image(
    row, timeout, user_agent_token, disallowed_header_directives, max_data_size=None
):
    """Download an image with urllib"""
    key, url = row
    img_stream = None
    user_agent_string = (
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:72.0) Gecko/20100101 Firefox/72.0"
    )
    if user_agent_token:
        user_agent_string += f" (compatible; {user_agent_token}; +https://github.com/rom1504/img2dataset)"
    try:
        # print(url)
        request = urllib.request.Request(
            url, data=None, headers={"User-Agent": user_agent_string}
        )
        with urllib.request.urlopen(request, timeout=timeout) as r:
            if disallowed_header_directives and is_disallowed(
                r.headers,
                user_agent_token,
                disallowed_header_directives,
            ):
                return key, None, "Use of image disallowed by X-Robots-Tag directive"
            context_length = r.getheader("Content-Length")
            if (
                context_length is not None
                and max_data_size is not None
                and int(context_length) > max_data_size
            ):
                return key, None, "Image too large"
            img_stream = io.BytesIO(r.read())
        return key, img_stream, None
    except Exception as err:  # pylint: disable=broad-except
        if img_stream is not None:
            img_stream.close()
        return key, None, str(err)


def download_image_with_retry(
    row,
    timeout,
    retries,
    user_agent_token,
    disallowed_header_directives,
    max_data_size=None,
):
    for _ in range(retries + 1):
        key, img_stream, err = download_image(
            row, timeout, user_agent_token, disallowed_header_directives, max_data_size
        )
        if img_stream is not None:
            return key, img_stream, err
    return key, None, err


def compute_key(key, shard_id, oom_sample_per_shard, oom_shard_count):
    true_key = (10**oom_sample_per_shard) * shard_id + key
    key_format = oom_sample_per_shard + oom_shard_count
    str_key = "{true_key:0{key_format}d}".format(  # pylint: disable=consider-using-f-string
        key_format=key_format, true_key=true_key
    )
    return str_key


class Downloader:
    """The downloader class gets calls with shards, download them then call the writer to write them down"""

    def __init__(
        self,
        sample_writer_class,
        thread_count,
        save_caption,
        extract_exif,
        output_folder,
        column_list,
        timeout,
        number_sample_per_shard,
        oom_shard_count,
        compute_hash,
        verify_hash_type,
        encode_format,
        retries,
        user_agent_token,
        disallowed_header_directives,
        blurring_bbox_col=None,
        max_data_size=None,
    ) -> None:
        self.sample_writer_class = sample_writer_class
        self.thread_count = thread_count
        self.save_caption = save_caption
        self.output_folder = output_folder
        self.column_list = column_list
        self.timeout = timeout
        self.number_sample_per_shard = number_sample_per_shard
        self.oom_shard_count = oom_shard_count
        self.verify_hash_type = verify_hash_type
        self.encode_format = encode_format
        self.retries = retries
        self.user_agent_token = (
            None if user_agent_token is None else user_agent_token.strip().lower()
        )
        self.disallowed_header_directives = (
            None
            if disallowed_header_directives is None
            else {
                directive.strip().lower() for directive in disallowed_header_directives
            }
        )
        max_data_size = None if max_data_size is None else int(max_data_size) * 1000000
        self.max_data_size = max_data_size

    def __call__(
        self,
        row,
    ):
        try:
            self.download_shard(row)
            return (True, row)
        except Exception as err:  # pylint: disable=broad-except
            return (False, row)

    def download_shard(
        self,
        row,
    ):
        """Function to start an image downloading in one process"""

        shard_id, shard_file = row
        start_time = time.time()

        # check if shard_file exists

        fs, shard_path = fsspec.core.url_to_fs(shard_file)
        with open(shard_file, "rb") as f:
            df = pa.ipc.open_file(f).read_all()

        schema = df.schema
        schema = (
            schema.append(pa.field("key", pa.string()))
            .append(pa.field("status", pa.string()))
            .append(pa.field("error_message", pa.string()))
        )

        pydict = df.select(self.column_list).to_pydict()
        del df
        shard_to_dl = list(enumerate(zip(*(pydict[col] for col in self.column_list))))
        del pydict

        status_dict = CappedCounter()

        count = len(shard_to_dl)
        successes = 0
        failed_to_download = 0
        failed_to_resize = 0
        url_indice = self.column_list.index("url")
        caption_indice = (
            self.column_list.index("caption") if "caption" in self.column_list else None
        )

        key_url_list = [(key, x[url_indice]) for key, x in shard_to_dl]

        # this prevents an accumulation of more than twice the number of threads in sample ready to resize
        # limit the memory usage
        semaphore = Semaphore(self.thread_count * 2)

        def data_generator():
            for e in key_url_list:
                semaphore.acquire()  # pylint: disable=consider-using-with
                yield e

        loader = data_generator()

        # give schema to writer
        sample_writer = self.sample_writer_class(
            shard_id,
            self.output_folder,
            self.save_caption,
            self.oom_shard_count,
            schema,
            self.encode_format,
        )
        if not os.path.ismount(self.output_folder):
            raise ValueError(f"Output folder {self.output_folder} is not a mount point")
        oom_sample_per_shard = math.ceil(math.log10(self.number_sample_per_shard))
        with ThreadPool(self.thread_count) as thread_pool:
            for key, img_stream, error_message in thread_pool.imap_unordered(
                lambda x: download_image_with_retry(
                    x,
                    timeout=self.timeout,
                    retries=self.retries,
                    user_agent_token=self.user_agent_token,
                    disallowed_header_directives=self.disallowed_header_directives,
                ),
                loader,
            ):
                try:
                    _, sample_data = shard_to_dl[key]
                    str_key = compute_key(
                        key, shard_id, oom_sample_per_shard, self.oom_shard_count
                    )
                    meta = {
                        # Skip columns containing a the verification hash and only save the compute hash
                        **{
                            self.column_list[i]: sample_data[i]
                            for i in range(len(self.column_list))
                        },
                        "key": str_key,
                        "status": None,
                        "error_message": error_message,
                    }

                    if error_message is not None:
                        failed_to_download += 1
                        status = "failed_to_download"
                        status_dict.increment(error_message)
                        meta["status"] = status
                        sample_writer.write(
                            None,
                            str_key,
                            sample_data[caption_indice]
                            if caption_indice is not None
                            else None,
                            meta,
                        )
                        semaphore.release()
                        continue

                    img_stream.seek(0)
                    img = img_stream.read()

                    if error_message is not None:
                        failed_to_resize += 1
                        status = "failed_to_resize"
                        status_dict.increment(error_message)
                        meta["status"] = status
                        meta["error_message"] = error_message
                        sample_writer.write(
                            None,
                            str_key,
                            sample_data[caption_indice]
                            if caption_indice is not None
                            else None,
                            meta,
                        )
                        img_stream.close()
                        del img_stream
                        semaphore.release()
                        continue
                    successes += 1
                    status = "success"
                    status_dict.increment(status)

                    meta["status"] = status
                    img_stream.close()
                    del img_stream

                    sample_writer.write(
                        img,
                        str_key,
                        sample_data[caption_indice]
                        if caption_indice is not None
                        else None,
                        meta,
                    )
                except Exception as err:  # pylint: disable=broad-except
                    print(err)
                    # traceback.print_exc()
                    # logging.getLogger("download_shard").info(f"Sample {key} failed to download: {err}")
                semaphore.release()

            sample_writer.close()
            thread_pool.terminate()
            thread_pool.join()
            del thread_pool

        end_time = time.time()
        write_stats(
            self.output_folder,
            shard_id,
            count,
            successes,
            failed_to_download,
            failed_to_resize,
            start_time,
            end_time,
            status_dict,
            self.oom_shard_count,
        )
        fs.rm(shard_path)


def arguments_validator(params):
    """Validate the arguments"""
    if params["compute_hash"] not in [None, "md5", "sha256", "sha512"]:
        hash_type = params["compute_hash"]
        raise ValueError(f"Unsupported hash to compute: {hash_type}")

    if params["verify_hash"] is not None:
        _, verify_hash_type = params["verify_hash"]
        if verify_hash_type != params["compute_hash"]:
            raise ValueError(
                "verify_hash and compute_hash must be the same "
                f"but got {verify_hash_type} and {params['compute_hash']}"
            )

    if params["save_additional_columns"] is not None:
        save_additional_columns_set = set(params["save_additional_columns"])

        forbidden_columns = set(
            [
                "key",
                "caption",
                "url",
                "width",
                "height",
                "original_width",
                "original_height",
                "status",
                "error_message",
                "exif",
                "md5",
                "sha256",
                "sha512",
            ]
        )
        intersection = save_additional_columns_set.intersection(forbidden_columns)
        if intersection:
            raise ValueError(
                f"You cannot use in save_additional_columns the following columns: {intersection}."
                + "img2dataset reserves these columns for its own use. Please remove them from save_additional_columns."
            )


def download(
    url_list: str,
    image_size: int = 256,
    output_folder: str = "images",
    processes_count: int = 1,
    resize_mode: str = "border",
    resize_only_if_bigger: bool = False,
    upscale_interpolation: str = "lanczos",
    downscale_interpolation: str = "area",
    encode_quality: int = 95,
    encode_format: str = "jpg",
    skip_reencode: bool = False,
    output_format: str = "files",
    input_format: str = "txt",
    url_col: str = "url",
    caption_col: Optional[str] = None,
    bbox_col: Optional[str] = None,
    thread_count: int = 256,
    number_sample_per_shard: int = 10000,
    extract_exif: bool = True,
    save_additional_columns: Optional[List[str]] = None,
    timeout: int = 10,
    enable_wandb: bool = False,
    wandb_project: str = "img2dataset",
    oom_shard_count: int = 5,
    compute_hash: Optional[str] = "sha256",
    verify_hash: Optional[List[str]] = None,
    distributor: str = "multiprocessing",
    subjob_size: int = 100,
    retries: int = 0,
    disable_all_reencoding: bool = False,
    min_image_size: int = 0,
    max_image_area: float = float("inf"),
    max_aspect_ratio: float = float("inf"),
    incremental_mode: str = "incremental",
    max_shard_retry: int = 1,
    user_agent_token: Optional[str] = None,
    disallowed_header_directives: Optional[List[str]] = None,
    start_file_id: int = 0,
    max_data_size: Optional[int] = None,
):
    """Download is the main entry point of img2dataset, it uses multiple processes and download multiple files"""
    if disallowed_header_directives is None:
        disallowed_header_directives = ["noai", "noimageai", "noindex", "noimageindex"]
    if len(disallowed_header_directives) == 0:
        disallowed_header_directives = None

    config_parameters = dict(locals())
    arguments_validator(config_parameters)

    def make_path_absolute(path):
        fs, p = fsspec.core.url_to_fs(path)
        if fs.protocol == "file":
            return os.path.abspath(p)
        return path

    output_folder = make_path_absolute(output_folder)
    url_list = make_path_absolute(url_list)

    logger_process = LoggerProcess(
        output_folder, enable_wandb, wandb_project, config_parameters
    )

    tmp_path = output_folder + "/_tmp"
    fs, tmp_dir = fsspec.core.url_to_fs(tmp_path)
    if not fs.exists(tmp_dir):
        fs.mkdir(tmp_dir)

    def signal_handler(signal_arg, frame):  # pylint: disable=unused-argument
        try:
            fs.rm(tmp_dir, recursive=True)
        except Exception as _:  # pylint: disable=broad-except
            pass
        logger_process.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    save_caption = caption_col is not None

    fs, output_path = fsspec.core.url_to_fs(output_folder)
    start_shard_id = 0

    if not fs.exists(output_path):
        fs.mkdir(output_path)
        done_shards = set()
    else:
        if incremental_mode == "incremental":
            done_shards = set(
                int(x.split("/")[-1].split("_")[0])
                for x in fs.glob(output_path + "/*.json")
            )
        elif incremental_mode == "overwrite":
            fs.rm(output_path, recursive=True)
            fs.mkdir(output_path)
            done_shards = set()
        elif incremental_mode == "extend":
            existing_shards = [
                int(x.split("/")[-1].split("_")[0])
                for x in fs.glob(output_path + "/*.json")
            ]
            start_shard_id = max(existing_shards, default=-1) + 1
            done_shards = set()
        else:
            raise ValueError(f"Unknown incremental mode {incremental_mode}")

    logger_process.done_shards = done_shards
    logger_process.start()

    if bbox_col is not None:
        if save_additional_columns is None:
            save_additional_columns = [bbox_col]
        else:
            save_additional_columns.append(bbox_col)

    if verify_hash is not None:
        verify_hash_col, verify_hash_type = verify_hash
    else:
        verify_hash_col = None
        verify_hash_type = None

    reader = Reader(
        url_list,
        input_format,
        url_col,
        caption_col,
        verify_hash_col,
        verify_hash_type,
        save_additional_columns,
        number_sample_per_shard,
        done_shards,
        tmp_path,
        start_shard_id,
        start_file_id,
    )

    if output_format == "webdataset":
        sample_writer_class = WebDatasetSampleWriter
    elif output_format == "parquet":
        sample_writer_class = ParquetSampleWriter  # type: ignore
    elif output_format == "files":
        sample_writer_class = FilesSampleWriter  # type: ignore
    elif output_format == "tfrecord":
        sample_writer_class = TFRecordSampleWriter  # type: ignore
    elif output_format == "dummy":
        sample_writer_class = DummySampleWriter  # type: ignore
    else:
        raise ValueError(f"Invalid output format {output_format}")

    downloader = Downloader(
        sample_writer_class=sample_writer_class,
        thread_count=thread_count,
        save_caption=save_caption,
        extract_exif=extract_exif,
        output_folder=output_folder,
        column_list=reader.column_list,
        timeout=timeout,
        number_sample_per_shard=number_sample_per_shard,
        oom_shard_count=oom_shard_count,
        compute_hash=compute_hash,
        verify_hash_type=verify_hash_type,
        encode_format=encode_format,
        retries=retries,
        user_agent_token=user_agent_token,
        disallowed_header_directives=disallowed_header_directives,
        blurring_bbox_col=bbox_col,
    )

    print("Starting the downloading of this file")
    if distributor == "multiprocessing":
        distributor_fn = multiprocessing_distributor
    elif distributor == "pyspark":
        distributor_fn = pyspark_distributor
    elif distributor == "ray":
        distributor_fn = ray_distributor
    else:
        raise ValueError(f"Distributor {distributor} not supported")

    distributor_fn(
        processes_count,
        downloader,
        reader,
        subjob_size,
        max_shard_retry,
    )
    logger_process.join()

    if not hasattr(fs, "s3"):
        fs.rm(tmp_dir, recursive=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--master_node")
    parser.add_argument("--output_dir")
    parser.add_argument("--url_list")
    parser.add_argument("--num_proc")
    parser.add_argument("--caption_col")
    parser.add_argument("--url_col")
    parser.add_argument("--encode_format")

    args = parser.parse_args()

    master_node = args.master_node
    print(master_node)

    processes_count = args.num_proc

    output_dir = args.output_dir
    print(output_dir)

    url_list = args.url_list

    try:
        spark = spark_session(master_node, processes_count, 256)
    except Exception as e:
        pass

    try:
        download(
            processes_count=processes_count,
            thread_count=32,
            url_list=url_list,
            output_folder=output_dir,
            output_format="webdataset",
            input_format="parquet",
            url_col=args.url_col,
            caption_col=args.caption_col,
            enable_wandb=True,
            number_sample_per_shard=1000,
            distributor="ray",
            compute_hash=None,
            extract_exif=False,
            disable_all_reencoding=True,
            resize_mode="no",
            wandb_project="audio_download",
            subjob_size=1000,
            encode_format=args.encode_format,
            max_data_size=5,
        )

    except Exception as e:
        print(f"Error: {e}")
        print(traceback.format_exc())
        raise e
