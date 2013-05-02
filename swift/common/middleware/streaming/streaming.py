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
        environ['HTTP_RANGE'] = 'bytes=0-4194304'
        def start_response(status, headers, *args):
            print 'start request start_response(%r, %r)' % (status, headers)
            environ['swift.start_response'] = (status, headers)

        return self.app(environ, start_response)

    def make_tail_request(self, environ, pos):
        environ['HTTP_RANGE'] = 'bytes=%s-' % pos
        def start_response(status, headers, *args):
            print 'tail request start_response(%r, %r)' % (status, headers)
            environ['swift.tail_response'] = (status, headers)

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
            start_file = StringIO(''.join(start_resp))
            status, headers = environ['swift.start_response']
            for header, value in headers:
                # 'content-range', 'bytes 0-4194304/72726016'
                if header == 'content-range':
                    content_length = int(value.split('/')[-1])
                elif header == 'content-type':
                    content_type = value
            # parse mp4
            mp4stream = SwiftStreamMp4(start_file, content_length, start_param)
            print 'PARSE MP4'
            mp4stream._parseMp4()
            # pos = str(e).split()[-1]
            # Update StreamAtom elements
            print 'UPDATE ATOMS'
            mp4stream._updateAtoms()
            status = '200 OK'
            # FIXME can I come up with a content-length here?
            headers = [('content-type', content_type)]
            # call start response
            print 'calling start_response(%r, %r)' % (status, headers)
            start_response(status, headers)
            # Yield to Stream
            print 'WRITE NEW'
            def body_iter():
                for chunk in mp4stream._yieldToStream():
                    yield chunk
                for chunk in self.tail_resp(mp4stream.atoms.jump_to_pos):
                    yield chunk
            return body_iter()
        else:
            # handle non-start-param request un-molested
            return self.app(environ, start_response)


def filter_factory(local_conf, **global_conf):
    def app_filter(app):
        return StreamFilter(app)
    return app_filter
