import sys
from kbstorage import KBFile, to_str

command = sys.argv[1]
args = sys.argv[2:]
if command == "get":
    path, key = args
    f = KBFile.open(path)
    sys.stdout.write(f[key].decode("utf-8"))
elif command == "put":
    path, key, data = args
    f = KBFile.open(path)
    f[key] = data
elif command == "create":
    path = args[0]
    f = KBFile.create(path)
elif command == "del":
    path, key = args
    f = KBFile.open(path)
    del f[key]
    
elif command == "ls":
    path = args[0]
    f = KBFile.open(path)
    for k in f.keys():
        if isinstance(k, bytes):
            k = k.decode("utf-8")
        print("%-40s %s" % (k, f.blob_size(k)))
        
elif command == "dump":
    path = args[0]
    f = KBFile.open(path)
    print("Header:")
    print("  Signature:         ", f.Signature)
    print("  Version:            %s.%s" % f.Version)
    print("  Data offset:       ", f.DataOffset)
    print("  Directory offset:  ", f.DirectoryOffset)
    print()

    print("Data:")
    directory = f.directory()
    maxkey = 10
    if directory:
        maxkey = max(maxkey, max(len(k) for k, _, _ in directory))
    
    fmt = f"%-{maxkey}s %12s %12s"
    header = fmt % ("Key", "Offset", "Size")
    print(header)
    print("-"*(maxkey+12+12+2))
    for i, (k, o, s) in enumerate(directory):
        print(fmt % (to_str(k), o, s))
        if i < len(directory)-1:
            _, o1, _ = directory[i+1]
            if o + s < o1:
                print(fmt % ("<-gap->", "", o1 - o - s))
    print()
    
    print("Free space:")
    print("  Offset:            ", f.FreeSpace)
    print("  Size:              ", f.DirectoryOffset-f.FreeSpace)
    print()
    
    print("Directory:")
    print("  Entries:           ", len(f.Directory))
    print("  Size:              ", f.FileSize - f.DirectoryOffset)
    print()
    print("File size:           ", f.FileSize)
