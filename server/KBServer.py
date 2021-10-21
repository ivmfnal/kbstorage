from webpie import WPApp, WPHandler
from kbstorage import KBCachedStorage, to_bytes
import sys, re, zlib
from urllib.parse import unquote

class Handler(WPHandler):
    
    def stream_blob(self, blob, chunk_size=16*1024):
        n = len(blob)
        for i in range(0, chunk_size, n):
            yield blob[i:i+chunk_size]

    def stream_as_chunks(self, data, chunk_size=16*1024):
        chunk = []
        n = 0
        for x in data:
            if n >= chunk_size:
                yield b''.join(chunk)
                n = 0
                chunk = []
            if len(x):
                if isinstance(x, str):
                    x = x.encode("utf-8")
                chunk.append(x)
                n += len(x)
        if chunk:
            yield b''.join(chunk)

    def get(self, request, relpath, key=None, compress="yes", **args):
        key = key or relpath
        key = key.encode("utf-8")
        compress = compress == "yes"
        try:
            blob = self.App.DB[key]
        except KeyError:
            return 404
        content_type = "application/octet-stream"
        if compress:
            content_type = "application/zip"
            blob = zlib.compress(blob)
        return blob, 200, content_type, {"Content-Length":len(blob)}

    def put(self, request, relpath, key=None, **args):
        key = to_bytes(key or relpath) or None
        blob = to_bytes(request.body)
        key = self.App.DB.add_blob(key, blob)
        return key

    def blob(self, request, relpath, **args):
        if request.method.lower() == "get":
            return self.get(request, relpath, **args)
        else:
            return self.put(request, relpath, **args)

    def reload(self, request, relpath, **args):
        self.App.DB.reload()
        return "OK"

    def keys(self, request, relpath, key=None, pattern=None, min_key=None, max_key=None, **args):
        key = key or relpath
        pattern_re = None
        if pattern:
            pattern_re = re.compile(unquote(pattern))
        def filter_keys(keys):
            for k in keys:
                if isinstance(k, bytes):
                    k = k.decode("utf-8")
                if pattern_re and not pattern_re.match(k):
                    continue
                if min_key and k < min_key:
                    continue
                if max_key and k >= max_key:
                    continue
                yield k + "\n"
        return self.stream_as_chunks(filter_keys(self.App.DB.keys())), 200, "text/csv"
    
    COMPRESS_LIMIT = 1024
    
    def get_bulk(self, request, relpath, keys=None, compress="yes", **args):
        keys = keys or relpath
        if keys:
            keys = keys.split(",")
        elif request.headers["Content-Type"] == "text/csv":
            keys = [k.strip() for k in request.body.split(b"\n")]
            keys = [k for k in keys if k]
        else:
            keys = json.load(request.body_file)
        compress = compress == "yes"
        
        def stream_data(pairs):
            def format_blob(key, blob):
                compressed = False
                orig_size = len(blob)
                if compress and orig_size >= self.COMPRESS_LIMIT:
                    compressed = True
                    orig_size = len(blob)
                    blob = zlib.compress(blob)
                flags = ("z" if compressed else "-") + ","      # flags + specs delimiter
                header = to_bytes("%s %s %d:" % (flags, key, len(blob)))
                return header + blob
        
            for key, blob in pairs:
                yield format_blob(key, blob)
        
        pairs = self.App.DB.blobs(keys)
        return stream_data(pairs), 200, "application/octet-stream; charset=utf-8"

class App(WPApp):
    
    def __init__(self, storage_path):
        WPApp.__init__(self, Handler)
        self.DB = KBCachedStorage(storage_path)

if __name__ == "__main__":
    import getopt, sys
    
    opts, args = getopt.getopt(sys.argv[1:], "p:s:")
    opts = dict(opts)
    port = int(opts.get("-p", 8888))
    storage = opts["-s"]
    
    print("Starting on port", port)
    App(storage).run_server(port)
