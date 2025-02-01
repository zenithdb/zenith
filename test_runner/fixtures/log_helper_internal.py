# These are logically part of in log_helper.py, but need to be in a
# different file because these get loaded from the logging config
# file. If you try to included these in log_helper.py directly, you
# get an error about circular dependency.
import re

class WerkzeugNoiseFilter(object):
    """Moto server that we use for mocking S3 uses werkzeug, which
    logs all HTTP operations.  It constructs log messages like this:

        127.0.0.1 - - [08/Oct/2024 12:43:46] "PUT /bucket-name/path?x-id=PutObject HTTP/1.1" 200 -

    The IP address is not interesting in tests, as it's always just
    127.0.0.1. And the timestamp is redundant with the timestamp we
    print for all log messages anyway, with millisecond precision.
    Unfortunately those are "etched" in the message, and cannot be
    overriden by setting a custom formatter. To reduce the noise in
    the test output, this filter removes those fields from the log
    messages.
    """

    def filter(self, logRecord):
        logRecord.msg = re.sub(r'127\.0\.0\.1 - - \[.+\] (".*".*)', r'\1', logRecord.msg)
        return True
