from webpie import WPApp, WPHandler
from kbstorage import KBStorage

class Handler(WPHanlder):
    
    def get(self, request, relpath, key=None, **args):
        try:
            blob = 