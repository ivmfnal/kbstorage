import struct, json

BYTE_ORDER = '!'
Version = "1.0"

class FileSizeLimitExceeded(Exception):
    pass

class KBFile(object):
    
    PAGE_SIZE = 8*1024
    FORMAT_VERSION = (1,0)
    ZERO_PAGE = b'\0' * PAGE_SIZE
    SIGNATURE = b"KbF!"
    MAX_FILE_SIZE = 512*1024*1024       # 0.5GB
    
    #
    # File format:
    #
    #    0: Header 1 page (PAGE_SIZE)
    #       format version - 2 bytes
    #       data offset - 8 bytes (=PAGE_SIZE)
    #       directory offset - 8 bytes
    #    PAGE-SIZE: Data, multiple of PAGE_SIZE
    #    <directory offset>:
    #        odrederd by offset
    #        <lenmask><offset><size><key length>[4]<key>
    #        ...
    #
    #   lenmask: 4 upper bits: a, 4 lower bits: b
    #       offset is stored as 2^a unsigned byte integer
    #       size is stored as 2^b unsigned byte integer
    #
    
    def __init__(self, path, name=None):
        self.Name = name or path.rsplit("/",1)[-1].split(".", 1)[0]
        self.Path = path
        self.F = None
        self.Directory = {}         # key -> (offset, size)
        self.DataOffset = self.DirectoryOffset = None
        self.FreeSpace = None
        self.FileSize = None
        
    def _open(self):
        self.F = open(self.Path, "r+b")
        self.Name = self.Path.rsplit("/",1)[-1].split(".", 1)[0]
        self.FreeSpace = self.PAGE_SIZE
        self.read_directory()
        self.FileSize = self.F.tell()
        
    def _init(self):
        self.F = open(self.Path, "w+b")
        self.FreeSpace = self.DataOffset = self.PAGE_SIZE
        self.DirectoryOffset = directory_offset = self.PAGE_SIZE * 2
        self.write_header()
        self.F.seek(self.DataOffset)
        self.F.write(self.ZERO_PAGE)    # data
        self.F.truncate()
        self.FileSize = self.F.tell()
        
    @staticmethod
    def open(path):
        #print(f"open({path})")
        f = KBFile(path)
        f._open()
        return f
        
    @staticmethod
    def create(path, name=None):
        f = KBFile(path, name=name)
        f._init()
        return f
        
    def close(self):
        self.F.close()
        self.Directory = self.DataOffset = self.DirectoryOffset = self.FreeSpace = None

    def log8(self, x):
        l = 1
        u = 256
        while x > u:
            l += 1
            u *= 256
        #print("log8:", x, "->", l)
        return l
        
    def next_page_offset(self, n):
        return ((n + self.PAGE_SIZE - 1)//self.PAGE_SIZE)*self.PAGE_SIZE
        
    def pad_to_page(self, data, padding=b"\0"):
        n = len(data)
        to_pad = self.next_page_offset(n)
        if n < to_pad:
            data = data + (padding*(to_pad-n))
        return data

    def pack_offset_size(self, offset, size):
        off_log = self.log8(offset)
        size_log = self.log8(size)
        #print("pack_offset_size: offset, size:", offset, size, "   off_log, size_log:", off_log, size_log)
        assert off_log < 16 and size_log < 16        
        lenmask = (off_log << 4) + size_log
        off_len = 2**off_log
        size_len = 2**size_log
        parts = (
            struct.pack("!B", lenmask),
            offset.to_bytes(off_len, "big"),
            size.to_bytes(size_len, "big")
            )
        #print("   parts:", *(p.hex() for p in parts))
        out = b''.join(parts)
        #print("   out:", out.hex())
        return out
        
    def read_offset_size(self, data):
        print("unpack_offset_size: data:", bytes(data[:10]).hex())
        lenmask = int(data[0])
        off_log, size_log = (lenmask >> 4) & 15, lenmask & 15
        off_len = 2**off_log
        size_len = 2**size_log
        offset = int.from_bytes(data[1:1+off_len], "big")
        size = int.from_bytes(data[1+off_len:1+off_len+size_len], "big")
        #print("unpack_offset_size: returning:", offset, size, rest)
        return offset, size, 1+off_len+size_len
        
    def pack_header(self, directory_offset):
        header = (
            self.SIGNATURE
            + struct.pack("!BB", self.FORMAT_VERSION[0], self.FORMAT_VERSION[1]) 
            + self.PAGE_SIZE.to_bytes(8, "big")     # data offset
            + directory_offset.to_bytes(8, "big")    # directory offset
        )
        return self.pad_to_page(header)
        
    def pack_directory_entry(self, key, offset, size):
        if isinstance(key, str):
            key = key.encode("utf-8")
        out = self.pack_offset_size(offset, size) + struct.pack("!L", len(key)) + key
        #print("pack_directory_entry:", key, offset, size, " -> ", out.hex())
        return out
        
    def pack_directory(self):
        return b''.join([self.pack_directory_entry(key, offset, size)
                    for key, (offset, size) in self.Directory.items()
                ]
        )

    def write_header(self):
        self.F.seek(0,0)
        self.F.write(self.pack_header(self.DirectoryOffset))

    def read_header(self):
        self.F.seek(0,0)
        header = self.F.read(self.PAGE_SIZE)
        #print("header:", header)
        assert header[:len(self.SIGNATURE)] == self.SIGNATURE, "KB file signature not found: %s" % (repr(header[:len(self.SIGNATURE)]))
        i = len(self.SIGNATURE)
        version = header[i:i+2]
        i += 2
        self.DataOffset = int.from_bytes(header[i:i+8], "big")
        i += 8
        self.DirectoryOffset = int.from_bytes(header[i:i+8], "big")
        i += 8
        #print("read_header:")
        #print("    version:", version)
        #print("    data offset:", self.DataOffset)
        #print("    directory offset:", self.DirectoryOffset)

    @property
    def data_offset(self):
        if self.DataOffset is None:
            self.read_header()
        return self.DataOffset
        
    @property
    def size(self):
        return self.FreeSpace - self.DataOffset

    @property
    def directory_offset(self):
        if self.DirectoryOffset is None:
            self.read_header()
        return self.DirectoryOffset

    def read_directory_entry(self, data):
        #        <lenmask><offset><size><key length>[4]<key>
        lenmask = int(data[0])
        #print("read_directory_entry: lenmask: %x" % (lenmask,))
        off_log, size_log = (lenmask >> 4) & 15, lenmask & 15
        off_len = 2**off_log
        size_len = 2**size_log
        #print("read_directory_entry: off_len, size_len:", off_log, size_log)
        offset = int.from_bytes(data[1:1+off_len], "big")
        size = int.from_bytes(data[1+off_len:1+off_len+size_len], "big")
        #print("    offset, size:", offset, size)
        i = 1+off_len+size_len
        (keylen,) = struct.unpack_from("!L", data, i)
        #print("    keylen:", keylen)
        i += 4
        key = data[i:i+keylen]
        return offset, size, key, i+keylen
            
    def read_directory(self):
        self.F.seek(self.directory_offset, 0)
        data = self.F.read()
        i = 0
        n = len(data)
        #print(f"read_directory: dir data ({n}):", data[:20].hex(), data[:20])
        self.Directory = {}
        view = memoryview(data)
        l = len(view)
        while i < l:
            offset, size, key, consumed = self.read_directory_entry(view[i:])
            key = bytes(key)
            self.Directory[key] = (offset, size)
            self.FreeSpace = max(self.FreeSpace, offset+size)            
            #print("    key:", key)
            i += consumed
            
    def write_directory(self, offset):
        self.F.seek(offset, 0)
        self.F.write(self.pack_directory())
        self.F.truncate()
            
    def append_blob(self, key, blob, offset):
        # assume there is enough room to store the blob at given offset
        #print(f"append_blob({key}) at {offset}")
        self.F.seek(offset, 0)
        self.F.write(blob)
        self.FreeSpace = self.F.tell()
        self.F.seek(0, 2)
        self.F.write(self.pack_directory_entry(key, offset, len(blob)))
        self.F.truncate()
        self.Directory[key] = (offset, len(blob))

    def add_blob(self, key, blob):
        if isinstance(key, str):
            key = key.encode("utf-8")
        l = len(blob)
        if not self.Directory:
            self.read_directory()
        free_space = self.DirectoryOffset - self.FreeSpace
        dir_offset = self.DirectoryOffset
        while free_space < len(blob):
            dir_offset += self.PAGE_SIZE
            free_space += self.PAGE_SIZE
        if dir_offset > self.MAX_FILE_SIZE:
            raise FileSizeLimitExceeded()
        if dir_offset > self.DirectoryOffset:
            self.DirectoryOffset = dir_offset
            self.write_directory(dir_offset)
            self.write_header()
        self.append_blob(key, blob, self.FreeSpace)
        
    __setitem__ = add_blob
    
    def get_blob(self, key):
        if isinstance(key, str):
            key = key.encode("utf-8")
        offset, size = self.Directory[key]
        self.F.seek(offset)
        blob = self.F.read(size)
        return blob
        
    __getitem__ = get_blob
    
    def blob_size(self, key):
        if isinstance(key, str):
            key = key.encode("utf-8")
        offset, size = self.Directory[key]
        return size
        
    def meta(self, key):
        return {"size":self.blob_size(key)}
    
    def keys(self):
        return self.Directory.keys()
        
    def items(self):
        for k in self.keys():
            yield k, self[k]

    def __delitem__(self, key):
        if isinstance(key, str):
            key = key.encode("utf-8")
        del self.Directory[key]
        self.write_directory(self.DirectoryOffset)
        
    def compactable(self):
        entries = sorted([(offset, size, key) for key, (offset, size) in self.Directory.items()])
        if not entries:
            return 0
        shift = 0
        end = entries[0][0]
            
        

    def compact(self):
        blobs = sorted([(offset, size, key) for key, (offset, size) in self.Directory.items()])
        new_directory = {}
        write_off = self.DataOffset
        for offset, size, key in blobs:
            if offset > write_off:
                self.F.seek(offset, 0)
                blob = self.F.read(size)
                self.F.seek(write_off, 0)
                self.F.write(blob)
            new_directory[key] = (write_off, size)
            write_off += size
        self.DirectoryOffset = self.next_page_offset(write_off)
        self.write_header()
        self.Directory = new_directory
        self.write_directory(self.DirectoryOffset)

if __name__ == "__main__":
    import sys
    
    command = sys.argv[1]
    args = sys.argv[2:]
    if command == "get":
        path, key = args
        f = KBFile.open(path)
        sys.stdout.write(f[key].decode("utf-8"))
    elif command == "put":
        path, key, infile = args
        data = open(infile, "rb").read()
        f = KBFile.open(path)
        f[key] = data
    elif command == "create":
        path = args[0]
        f = KBFile.create(path)
    elif command == "ls":
        path = args[0]
        f = KBFile.open(path)
        for k in f.keys():
            if isinstance(k, bytes):
                k = k.decode("utf-8")
            print("%-40s %s" % (k, f.blob_size(k))) 
        
        
    
    
    
    
            

        