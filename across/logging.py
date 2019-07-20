import logging

from . import get_connection, DisconnectError
from ._utils import logger as _across_logger


_across_logger_name = _across_logger.name
_default_formatter = logging.Formatter()


class AcrossHandler(logging.Handler):
    def __init__(self, conn=None):
        super(AcrossHandler, self).__init__()
        if conn is None:
            conn = get_connection()
        self.__conn = conn

    def emit(self, record):
        try:
            if record.name == _across_logger_name:
                return

            # there are a few things that need to be done before serializing record:
            #   - args may not be pickleable, so we need to format message locally;
            #   - tracebacks are not pickleable, so we need to format exception locally;
            #   - standard formatter defines some temporary attributes on records: record.message and
            #     record.asctime; as an optimization, we omit them during serialization
            if record.exc_info and not record.exc_text:
                record.exc_text = (self.formatter or _default_formatter).formatException(record.exc_info)
            attrs = record.__dict__.copy()
            message = attrs.pop('message', None)
            if message is None:
                message = record.getMessage()
            attrs['msg'] = message
            attrs['args'] = ()
            attrs['exc_info'] = None
            attrs.pop('asctime', None)

            self.__conn.call(_handle, attrs)
        except DisconnectError:
            pass
        except Exception:
            self.handleError(record)


def _handle(attrs):
    record = logging.makeLogRecord(attrs)
    logging.getLogger(record.name).handle(record)
