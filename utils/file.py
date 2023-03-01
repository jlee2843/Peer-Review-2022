import Path from pathlib

__basePathName = None
__path = None
__filename = None

def setBasePathName(basepath: str):
    global __basePathName
    
    if basepath.endswith('/') == False:
        basepath = basepath + '/'
    __basePathName = basepath

    p = Path(basepath)
    p.mkdir(parents = True, exist_ok = True)
    
def getBasePathName() -> str:
    global __basePathName
    
    
    return __basePathName
    
def setPath(path: str):
    global __path
    
    if path.endswith('/') == False:
        path = path + '/'
    __path = path
    
def getPath(basePath:str = '') -> str:
    global __path
    
    if __path is None:
        setPath(f'{datetime.now()}/')

    p = Path(f'{basePath}{__path}')
    p.mkdir(parents = True, exist_ok = True)

    return f'{basePath}{__path}'

def setFileName(filename: str):
    global __filename
    
    __filename = filename
    
def getFileName(path: str = '') -> str:
    from datetime import datetime
    
    global __filename
    
    if __filename is None:
        setFileName(f'{datetime.utcnow().timestamp()}.parquet')
        
    return f'{path}{__filename}'

#from configparser import ConfigParser
#from itertools import chain

#parser = ConfigParser()
#with open("foo.conf") as lines:
#    lines = chain(("[top]",), lines)  # This line does the trick.
#    parser.read_file(lines)
