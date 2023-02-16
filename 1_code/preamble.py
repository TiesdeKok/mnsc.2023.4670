
# ---------------------------------------------------------------------------- #
#                               Check environment                              #
# ---------------------------------------------------------------------------- #

import os
warning_msg = 'Warning: it does not appear you are using the {0} environment.'
if os.environ['CONDA_DEFAULT_ENV'] != CONDA_ENVIRONMENT:
    print(warning_msg.format(CONDA_ENVIRONMENT))

# ---------------------------------------------------------------------------- #
#                    Imports that should always be available                   #
# ---------------------------------------------------------------------------- #

import matplotlib.pyplot as plt

import re, math, time, sys, json, yaml, copy, orjson, random
from pathlib import Path
import pandas as pd
import numpy as np
from pqdm.threads import pqdm as pqdm_t
from pqdm.processes import pqdm as pqdm_p
from tqdm.notebook import tqdm
from joblib import Parallel, delayed
import requests

from IPython.display import HTML

# ---------------------------------------------------------------------------- #
#                                   Settings                                   #
# ---------------------------------------------------------------------------- #

# ------------------------------ Pandas settings ----------------------------- #

pd.options.mode.chained_assignment = None  # default='warn'
pd.set_option('display.float_format', lambda x: f'{x:,.2f}')

# -------------------------- Disable warning message ------------------------- #

def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn

# ---------------------------------------------------------------------------- #
#                             Set working directory                            #
# ---------------------------------------------------------------------------- #

workdir = re.sub("(?<={})[\w\W]*".format(PROJECT), "", os.getcwd())
os.chdir(workdir)

# ---------------------------------------------------------------------------- #
#                               Load config file                               #
# ---------------------------------------------------------------------------- #

with open(Path.cwd() / '1_code' / 'config.yml', 'r') as f:
    CONFIG = yaml.load(f)

# ---------------------------------------------------------------------------- #
#                          Set up pipeline directories                         #
# ---------------------------------------------------------------------------- #

# ------------------------------ Local pipeline ------------------------------ #

if os.path.exists(Path.cwd() / 'empirical' / '2_pipeline'):
    pipeline = Path.cwd() / 'empirical' / '2_pipeline' / NAME
else:
    pipeline = Path.cwd() / '2_pipeline' / NAME
    
if not os.path.exists(pipeline):
    os.makedirs(pipeline)
    for folder in ['out', 'store', 'tmp']:
        os.makedirs(pipeline / folder)

# ----------------------------- External pipeline ---------------------------- #

if USE_EXTERNAL_PIPELINE:
    externalPipelineFolder = Path(CONFIG['directories']['externalPipeline'][USER]) / PROJECT
    ePipeline = externalPipelineFolder / NAME

    if not os.path.exists(ePipeline):
        os.makedirs(ePipeline)
        for folder in ['out', 'store', 'tmp']:
            os.makedirs(ePipeline / folder)

# ---------------------------------------------------------------------------- #
#                            Load general functions                            #
# ---------------------------------------------------------------------------- #

import importlib.util
from inspect import getmembers, isfunction

function_file_path = Path.cwd() / '1_code' / 'functions.py'

spec = importlib.util.spec_from_file_location("functions", function_file_path)
functions = importlib.util.module_from_spec(spec)
spec.loader.exec_module(functions)
    
functions_list = getmembers(functions, isfunction)
functions_list = [x[0] for x in functions_list]

# ---------------- Print message to notebook for replicability --------------- #

to_exclude = ['loadTest']
msg = f'The following utility functions are loaded and available through `functions.<..>`:'
print('-' * len(msg), msg, '-' * len(msg), sep ='\n', end='\n\n')
print(', '.join([m for m in functions_list if m not in to_exclude]), end='\n\n')

# ---------------------------------------------------------------------------- #
#          Print message to display imported modules for replicability         #
# ---------------------------------------------------------------------------- #

from types import ModuleType as MT
from types import FunctionType as FT

imported_modules = [k for k,v in globals().items() if type(v) in [MT, FT] and not k.startswith('__')]
imported_modules = sorted(imported_modules)

# ---------------- Print message to notebook for replicability --------------- #

to_exclude = ['reload', 'getmembers', 'isfunction', 'warn', 'warnings', 'functions']
msg = f'The following modules and functions are imported by preamble.py:'
print('-' * len(msg), msg, '-' * len(msg), sep ='\n', end='\n\n')
print(', '.join([m for m in imported_modules if m not in to_exclude]))