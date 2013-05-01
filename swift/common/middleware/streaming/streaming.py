import urlparse
from StringIO import StringIO

from swift.common import swob
from swift.common.middleware.streaming.mp4stream.StreamMp4 import SwiftStreamMp4


class StreamFilter(object):

    def __init__(self, app):
        self.app = app

    def make_start_request(self, environ):
        """
        request the first 4MB of the object and return the response
        """
        # FIXME everything is in memory right now
        # environ['HTTP_RANGE'] = 'bytes=0-4194304'
        def start_response(*args, **kwargs):
            print 'start request start_response(*%r, **%r)' % (args, kwargs)
            environ['swift.stream_response'] = (args, kwargs)
        return self.app(environ, start_response)

    def __call__(self, environ, start_response):
        # TODO remove debug
        print '*' * 50
        for k, v in environ.items():
            print '%s: %s' % (k, v)
        print '*' * 50
        print 'QUERY_STRING: %s' % environ.get('QUERY_STRING')
        print 'RANGE: %s' % environ.get('HTTP_RANGE')

        parts = urlparse.parse_qs(environ.get('QUERY_STRING') or '')
        # TODO make the param name configurable
        start_param = parts.get('start', [''])[0]
        if start_param:
            # the client has specificially included a start param
            start_resp = self.make_start_request(environ)
            # parse mp4
            start_file = StringIO(''.join(start_resp))
            # FIXME content-length should be based on headers
            content_length = start_file.len
            mp4stream = SwiftStreamMp4(start_file, content_length, start_param)
            print 'PARSE MP4'
            mp4stream._parseMp4()
            # Update StreamAtom elements
            print 'UPDATE ATOMS'
            mp4stream._updateAtoms()
            # call start response
            args, kwargs = environ['swift.stream_response']
            print 'calling start_response(*%r, **%r)' % (args, kwargs)
            start_response(*args, **kwargs)
            # Yield to Stream
            print 'WRITE NEW'
            return mp4stream._yieldToStream()
        else:
            # handle non-start-param request un-molested
            return self.app(environ, start_response)


def filter_factory(local_conf, **global_conf):
    def app_filter(app):
        return StreamFilter(app)
    return app_filter
