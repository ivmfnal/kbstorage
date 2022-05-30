import struct, json

from .util import to_str, to_bytes, random_key

BYTE_ORDER = '!'
Version = "1.0"

class FileSizeLimitExceeded(Exception):
    pass

class KBFile(object):
    
    PAGE_SIZE = 8*1024
    SIZE_BYTES = 8              # length of size and offset fields in bytes: max file size, max blob size ~ 2**64 = 1.8e19
    KEY_SIZE_BYTES = 2                # length of key size field in bytes: max key size = 2**(8*2) = 65536
    SIGNATURE = b"KbF!"
    HEADER_SIZE = len(SIGNATURE) + 2 + 2*SIZE_BYTES     # signature + version + data_offset + directory_offset
    FORMAT_VERSION = (2,0)
    ZERO_PAGE = b'\0' * PAGE_SIZE
    MAX_FILE_SIZE = 1024*1024*1024       # 1GB
    
    #
    # File format:
    #   All integers (offsets, sizes) are stored in network (=big endian) format
    #   offset = 0: 
    #       Header
    #           signature = b"KbF!" - 4 bytes
    #           format version - 2 bytes (major, minor)
    #           data offset - 8 bytes (=HEADER_SIZE)
    #           directory offset - 8 bytes (SIZE_BYTES)
    #
    #   offset = HEADER_SIZE: 
    #       Data, multiple of PAGE_SIZE
    #       free space
    #
    #   offset = <directory offset>:
    #       odrederd by offset arrays of records:
    #           offset - 8 bytes   (SIZE_BYTES)
    #           size - 8 bytes     (SIZE_BYTES)
    #           key length - 2 bytes    (KEY_SIZE_BYTES)
    #           key - <key length>
    #           ...
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
        self.FreeSpace = self.DataOffset = self.HEADER_SIZE
        self.DirectoryOffset = directory_offset = self.FreeSpace + self.PAGE_SIZE
        self.write_header()
        self.write_directory()
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
        
    def pad(self, data, length, padding=b"\0"):
        n = len(data)
        padded_n = ((n+length-1)//length)*length
        if n < padded_n:
            data = data + (padding*(padded_n-n))
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
        #print("unpack_offset_size: data:", bytes(data[:10]).hex())
        lenmask = int(data[0])
        off_log, size_log = (lenmask >> 4) & 15, lenmask & 15
        off_len = 2**off_log
        size_len = 2**size_log
        offset = int.from_bytes(data[1:1+off_len], "big")
        size = int.from_bytes(data[1+off_len:1+off_len+size_len], "big")
        #print("unpack_offset_size: returning:", offset, size, rest)
        return offset, size, 1+off_len+size_len
        
    #       Header
    #           signature = b"KbF!" - 4 bytes
    #           format version - 2 bytes
    #           data offset - 8 bytes (=HEADER_SIZE)
    #           directory offset - 8 bytes (SIZE_BYTES)

    def write_header(self):
        header = (
            self.SIGNATURE
            + struct.pack("!BBQQ", self.FORMAT_VERSION[0], self.FORMAT_VERSION[1],
                self.DataOffset, self.DirectoryOffset
            ) 
        )
        self.F.seek(0,0)
        self.F.write(header)

    def read_header(self):
        self.F.seek(0,0)
        header = self.F.read(self.HEADER_SIZE)
        header = memoryview(header)
        
        assert header[:len(self.SIGNATURE)] == self.SIGNATURE, "KB file signature not found: %s" % (repr(header[:len(self.SIGNATURE)]))

        #print(len(header[len(self.SIGNATURE):]))
        v1, v0, data_offset, directory_offset = struct.unpack("!BBQQ", header[len(self.SIGNATURE):])
        #print("header: version:", v0, v1, "  data_offset:", data_offset, "  directory_offset:", directory_offset)
        assert data_offset == self.HEADER_SIZE
        self.DataOffset = data_offset
        self.DirectoryOffset = directory_offset

    
    #   offset = <directory offset>:
    #       odrederd by offset arrays of records:
    #           offset - 8 bytes   (SIZE_BYTES)
    #           size - 8 bytes     (SIZE_BYTES)
    #           key length - 2 bytes    (KEY_SIZE_BYTES)
    #           key - <key length>
    #           ...

    def pack_directory_entry(self, key, offset, size):
        if isinstance(key, str):
            key = key.encode("utf-8")
        return struct.pack("!QQH", offset, size, len(key)) + key
        
    def write_directory(self):
        offset = self.DirectoryOffset
        self.F.seek(offset, 0)
        for key, (offset, size) in self.Directory.items():
            self.F.write(self.pack_directory_entry(key, offset, size))
        self.F.truncate()

    def unpack_directory_entry(self, data):
        #print("unpack_directory_entry: data:", len(data))
        key_start = self.SIZE_BYTES + self.SIZE_BYTES + self.KEY_SIZE_BYTES
        offset, size, key_length = struct.unpack("!QQH", data[:key_start])
        key = data[key_start:key_start+key_length]
        return offset, size, bytes(key), key_start+key_length
            
    def read_directory(self):
        self.F.seek(self.directory_offset, 0)
        data = self.F.read()    # through the end of file
        i = 0
        n = len(data)
        #print(f"read_directory: dir data ({n}):", data[:20].hex(), data[:20])
        self.Directory = {}
        view = memoryview(data)
        l = len(view)
        while i < l:
            offset, size, key, consumed = self.unpack_directory_entry(view[i:])
            self.Directory[key] = (offset, size)
            self.FreeSpace = max(self.FreeSpace, offset+size)            
            i += consumed

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
        if key is None:
            key = random_key()
            while key in self.Directory:
                key = random_key()
        key = to_bytes(key)
        blob = to_bytes(blob)
        l = len(blob)
        if not self.Directory:
            self.read_directory()
            
        #
        # Try to squeeze the new blob between existing ones
        #
        
        blob_map = sorted(self.Directory.values())          # sorted by offset
        n = len(blob_map)
        last_i = n-1
        store_at = None
        for i, (offset, size) in enumerate(blob_map):
            if i < last_i:
                o1, s1 = blob_map[i+1]
                if o1 >= offset + size + l:
                    store_at = offset + size
                    break
        else:
            # append the blob to the end of data space, allocate more space if necessary, in page increments
            free_space = self.DirectoryOffset - self.FreeSpace
            dir_offset = self.DirectoryOffset
            while free_space < len(blob):
                dir_offset += self.PAGE_SIZE
                free_space += self.PAGE_SIZE
            if dir_offset > self.MAX_FILE_SIZE:
                raise FileSizeLimitExceeded()
            if dir_offset > self.DirectoryOffset:
                self.DirectoryOffset = dir_offset
                self.write_directory()
                self.write_header()
            store_at = self.FreeSpace
        self.append_blob(key, blob, store_at)
        return key
        
    __setitem__ = add_blob
    
    def get_blob(self, key):
        key = to_bytes(key)
        offset, size = self.Directory[key]
        self.F.seek(offset)
        blob = self.F.read(size)
        return blob
        
    __getitem__ = get_blob
    
    def blob_size(self, key):
        key = to_bytes(key)
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
        key = to_bytes(key)
        del self.Directory[key]
        self.write_directory()
        
    def directory(self):
        return sorted([(k, o, s) for k, (o, s) in self.Directory.items()], key=lambda x: x[1])
        
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
        self.write_directory()

        
    
    
    
    
            

        