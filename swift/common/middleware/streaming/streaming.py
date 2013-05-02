import urlparse
from StringIO import StringIO

from swift.common import swob
from swift.common.middleware.streaming.mp4stream.StreamMp4 import SwiftStreamMp4


class StreamFilter(object):

    def __init__(self, app):
        self.app = app

    def make_start_request(self, orig_environ):
        """
        Request the first 4MB of the Object and return the appropriate response
        TODO: The amount to request should become a configurable parameter
              since there is no guarantee that the required metadata of the MP4
              will be in the first 4 MB
        """
        environ = orig_environ.copy()
        environ['HTTP_RANGE'] = 'bytes=0-4194304'
        def start_response(status, headers, *args):
            print 'start request start_response(%r, %r)' % (status, headers)
            orig_environ['swift.start_response'] = (status, headers)

        return self.app(environ, start_response)

    def make_range_request(self, orig_environ, start, stop):
        """
        Returns a ranged request
        """
        environ = orig_environ.copy()
        environ['HTTP_RANGE'] = 'bytes=%s-%s' % (start, stop)
        def start_response(status, headers, *args):
            print 'start request start_response(%r, %r)' % (status, headers)
            if not status.startswith('2'):
                orig_environ['swift.range_error'] = True
            orig_environ['swift.range_response'] = (status, headers)

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
        # TODO make sure that the type of file requested *is* a mp4
        #      that way we can shortcut this 'expensive' operation
        # TODO make sure that start_param is numeric
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
            # ykim's edits are below with comments
            # We have to first verify that the parse successfully obtained
            # correct metadata
            if mp4stream._verifyMetadata():
                print 'UPDATING ATOMS'
                mp4stream._updateAtoms()
                status = '200 OK'
                # FIXME - Can I come up with a content-length here
                # ykim -Getting the content length here is going to be tricky since
                #       the whole file is now 'modified'. Actually... it might be do-able
                #       but I would need to know how 'necessary' it is
                headers = [('content-type', content_type)]
                print 'calling start_response(%r, %r)' % (status, headers)
                start_response(status, headers)
                def body_iter():
                    print 'Yielding chunks from start request'
                    # Yield the new metadata
                    for chunk in mp4stream._yieldMetadataToStream():
                        yield chunk
                    start, stop = mp4stream._getByteRangeToRequest()
                    print 'MAKING RANGE REQUEST: %s-%s' % (start, stop)
                    range_resp = self.make_range_request(environ, start, stop)
                    # Start yielding the main data
                    print 'Yielding chunks from range request'
                    for chunk in range_resp:
                        yield chunk
                return body_iter()
            else:
                # This should return some error or something
                # (i.e. ykim doesn't know what response to return)
                # ykim: I think the appropriate response is some 404 or something
                #       I'm not too entirely sure as nginx's response is an error
                #       The response here should be equivalent to Swift's error.
                raise Exception('ykim does not know what to do here')
        else:
            # handle non-start-param request un-molested
            return self.app(environ, start_response)


def filter_factory(local_conf, **global_conf):
    def app_filter(app):
        return StreamFilter(app)
    return app_filter
