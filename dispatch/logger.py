import boto3
import logging
import os
import sys

fmt = "%(asctime)s.%(msecs)03d:%(levelname)s:(%(name)s.%(funcName)s:%(lineno)d)[%(process)d] %(message)s"

log = logging.getLogger("dispatch")
log.propagate = False
log.setLevel(os.environ.get("LOG_LEVEL", "DEBUG"))
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    fmt=fmt, datefmt="%H:%M:%S"
)
handler.setFormatter(formatter)
log.addHandler(handler)

boto3.set_stream_logger('', logging.DEBUG, format_string=fmt)
