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
    directory = f.directory()
    for i, (k, o, s) in enumerate(directory):
        print(to_str(k), o, s)
        if i < len(directory)-1:
            _, o1, _ = directory(i+1)
            if o + s < o1:
                print("-gap-", o1 - o - s)

        
