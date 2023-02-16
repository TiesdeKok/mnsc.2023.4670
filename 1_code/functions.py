# ---------------------------------------------------------------------------- #
#                               Function imports                               #
# ---------------------------------------------------------------------------- #

import re, sys, os
import pandas as pd
import numpy as np
from pathlib import Path
import zipfile
import mgzip
import pathlib
import orjson

# ---------------------------------------------------------------------------- #
#                                 Test function                                #
# ---------------------------------------------------------------------------- #

def loadTest():
    print("Functions are loaded.") 

# ---------------------------------------------------------------------------- #
#          A function to check whether something exists in a database          #
# ---------------------------------------------------------------------------- #

def inDB(file, column, table, cur):
    res = cur.execute('SELECT EXISTS(SELECT 1 FROM {} WHERE {}="{}")'.format(table, column, file))
    if cur.fetchone()[0]:
        return True
    else:
        return False

# ---------------------------------------------------------------------------- #
#         A function to extract the sub-components out of an EDGAR link        #
# ---------------------------------------------------------------------------- #

def extract_data_edgar_link(link):
    parts = re.findall(r'data/(\d*?)/(\d*?)/(.*?)\.(.*?)$', link)[0]
    ret_dict = {
        'cik' : parts[0], 
        'cik_padded' : parts[0].zfill(10),
        'controlID' : parts[1], 
        'fileBase' : parts[2], 
        'link' : link, 
        'fileType' : parts[3],
        'fileName' : parts[2] + '.' + parts[3]
    }
    
    # ------------------------- Recreate fname from WRDS ------------------------- #

    cik = ret_dict['cik'] 
    control_id = ret_dict['controlID']
    fname = f'''edgar/data/{cik}/{control_id[:10]}-{control_id[10:12]}-{control_id[12:]}.txt'''
    ret_dict['fname'] = fname
    
    # ----------------------- Create save friendly uniqueID ---------------------- #

    ret_dict['uniqueID'] = ret_dict['cik'] + '-=-' + ret_dict['controlID'] + '-=-' + ret_dict['fileBase'] + '-=-' + ret_dict['fileType']
    
    return ret_dict

# ---------------------------------------------------------------------------- #
#          A function to recreate the EDGAR url based on the uniqueID          #
# ---------------------------------------------------------------------------- #

def recreate_edgar_link(uniqueID, clickable = False, prnt=False):
    labelList = ['cik', 'controlID', 'fileBase', 'fileType']
    data = {labelList[i] : v for i,v in enumerate(uniqueID.split('-=-'))}
    
    if clickable:
        link = 'https://www.sec.gov/Archives/edgar/data/{cik}/{controlID}/{fileBase}.{fileType}'.format(**data)
        if prnt:
            print(link)
    else:
        link = 'edgar/data/{cik}/{controlID}/{fileBase}.{fileType}'.format(**data)
    return link

# ---------------------------------------------------------------------------- #
#         A function to get rid of a multi-index in a Pandas dataframe         #
# ---------------------------------------------------------------------------- #

def flatten_multiindex_column(df):
    new_cols = []
    for col in df.columns.values:
        tupl = [x for x in col if len(x) > 0]
        if len(tupl) > 1:
            new_cols.append('_'.join([x.strip() for x in col]).strip())
        else:
            new_cols.append(tupl[0])
    df.columns = new_cols
    return df

# ---------------------------------------------------------------------------- #
#   The functions below automatically compress/decompress for faster storage   #
# ---------------------------------------------------------------------------- #

# ------------------------------ Store function ------------------------------ #

def fast_store_json(jsonObj, store_path, n_threads=0):
    json_string = orjson.dumps(jsonObj)
    
    ### mgzip only takes standard string paths
    if isinstance(store_path, pathlib.PosixPath) or isinstance(store_path, pathlib.WindowsPath):
        store_path = store_path.as_posix()

    with mgzip.open(store_path, 'wb', thread=n_threads, compresslevel=4, blocksize=2*10**8) as zipfile:
        zipfile.write(json_string)

# ------------------------------ Load functions ------------------------------ #

def fast_load_json(store_path, n_threads = 0):
    ### mgzip only takes standard string paths
    if isinstance(store_path, pathlib.PosixPath) or isinstance(store_path, pathlib.WindowsPath):
        store_path = store_path.as_posix()
    
    with mgzip.open(store_path, 'rb', thread=n_threads) as zipfile:
        my_object = zipfile.read()

    jsonObj = orjson.loads(my_object)
    return jsonObj