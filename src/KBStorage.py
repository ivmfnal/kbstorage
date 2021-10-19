from pythreader import Primitive, synchronized
import uuid, secrets, glob, os
from hashlib import sha1
from KBFile import KBFile, FileSizeLimitExceeded

def random_id(n=8):
    return secrets.token_hex(n)
    
def key_hash(key, modulo, level=0):
    if isinstance(key, str):
        key = key.encode("utf-8")
    h = int.from_bytes(sha1(key))
    return (h >> level) % modulo
    
class KBStorage(Primitive):
    
    def __init__(self, root_path, lock=None):
        Primitive.__init__(self, lock=lock)
        self.RootPath = root_path
        self.Files = {}     # name -> KBFile
        self.KeyMap = {}    # key -> file name
        self.CurrentFile = None     # file new entries are written to
        self.Cache = {}         # key -> blob
        self.CacheKeys = []     # keys sorted by access time
        self.load_files()
    
    def name_to_dir(self, name):
        x = name[-1]
        y = name[-2]
        return f"{self.RootPath}/{x}/{y}"
    
    def name_to_path(self, name):
        dir_path = self.name_to_dir(name)
        return f"{dir_path}/{name}.kbf"
        
    @synchronized
    def load_files(self):
        smallest_file = None
        smallest_size = None
        for path in glob.glob(f"{self.RootPath}/*/*/*.kbf"):
            f = KBFile.open(path)
            self.Files[f.Name] = f
            for k in f.keys():
                self.KeyMap[k] = f.Name
            size = f.size
            if smallest_file is None or size < smallest_size:
                smallest_file = f
                smallest_size = size
        self.CurrentFile = smallest_file
        #print("smallest file:", smallest_file.Name, smallest_size)
        if self.CurrentFile is None:
            self.CurrentFile = self.new_file()
            
    def keys(self):
        return self.KeyMap.keys()
        
    @synchronized
    def new_file(self):
        name = random_id()
        while name in self.Files:
            name = random_id()
        path = self.name_to_path(name)
        os.makedirs(path.rsplit("/",1)[0], exist_ok=True)
        self.Files[name] = f = KBFile.create(path, name)
        return f
    
    @synchronized
    def add_blob(self, key, blob):
        if self.CurrentFile is None:
            self.CurrentFile = self.new_file()
        f = self.CurrentFile
        try:
            f[key] = blob
        except FileSizeLimitExceeded:
            self.CurrentFile = f = self.new_file()
            f[key] = blob
        self.KeyMap[key] = self.CurrentFile.Name

    __setitem__ = add_blob
        
    @synchronized
    def get_blob(self, key):
        if isinstance(key, str):
            key = key.encode("utf-8")
        name = self.KeyMap[key]
        f = self.Files[name]
        return f[key]
        
    __getitem__ = get_blob
    
    @synchronized
    def meta(self, key):
        if isinstance(key, str):
            key = key.encode("utf-8")
        name = self.KeyMap[key]
        f = self.Files[name]
        return f.meta(key)

class LRUCache(Primitive):
    
    def __init__(self, capacity, data_source, lock=None):
        Primitive.__init__(self)
        self.Capacity = capacity
        self.DataSource = data_source
        self.Cache = {}
        self.CacheKeys = []
        
    @synchronized
    def __getitem__(self, key):
        if key in self.Cache:
            blob = self.Cache[key]
        else:
            blob = self.DataSource[key]
            self.Cache[key] = blob
        self.bump_key_and_clean_up(key)
        return blob
        
    @synchronized
    def __setitem__(self, key, blob):
        self.Cache[key] = blob
        self.DataSource[key] = blob
        self.bump_key_and_clean_up(key)

    def bump_key_and_clean_up(self, key):
        try:    self.CacheKeys.remove(key)
        except: pass
        self.CacheKeys.insert(0, key)
        while len(self.Cache) > self.Capacity:
            k = self.CacheKeys.pop()
            del self.Cache[k]
            
    def keys(self):
        return self.DataSource.keys()
        
    def meta(self, key):
        return self.DataSource.meta(key)

class KBCachedStorage(LRUCache):
    
    def __init__(self, root_path, cache_capacity=1000):
        storage = KBStorage(root_path)
        LRUCache.__init__(self, cache_capacity, storage)


if __name__ == "__main__":
    import getopt, sys
    
    Usage = """
    python storage.py <root> get <key>
                      <root> put <key> <file>
                      <root> ls
    """
    
    opts, args = getopt.getopt(sys.argv[1:], "")
    if not args:
        print(Usage)
        sys.exit(2)
        
    root, command, args = args[0], args[1], args[2:]
    storage = KBCachedStorage(root)
    
    if command == "get":
        key = args[0]
        blob = storage[key]
        sys.stdout.write(blob)
    
    elif command == "put":
        key, path = args
        blob = open(path, "rb").read()
        storage[key] = blob
        
    elif command == "ls":
        for k in storage.keys():
            meta = storage.meta(k)
            if isinstance(k, bytes):
                k = k.decode("utf-8")
            print("%-40s %d" % (k, meta["size"]))