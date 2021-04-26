# uses the pandas dataframe to make it easier for data scientiests to work with the ETL and results.
import pandas as pd
import numpy as np

# for viewing python code from functions
import inspect

# regex
import re

"""

using a python file of simple function definitions and an input csv file
you can create an excel like dependency tree to be fully calculated.

This is meant to be a python (pandas) driven alternative.


Basic usage : -------

function defintion script:
    Freeform define any and all functions

    function outputs are automatically mapped to inputs via matching names.
    For this reason name your funciton names and parameter names the same.

    Special uses:
        variables can be indexed by timestep. 
        ex:
            def f(t, x):
                return x[t]

            def g(t, x):
                return x[t+1]

            def h(t, x):
                return x[t-1]

runner script:
    import sketch.py

    # basic ETL of data
    # 1. Load data file into pandas DataFrame
    # 2. Load function definition script into Run object
    # 3. Run Calculation


---------------------



issue: 
    very wasteful with memory

    time-invariant scalers are coppied to every value of t.

issue:
    only considered one input

    inputs are only used if are loaded into the initial DateFrame.

issue:
    doesn't handle inforce files yet

    the initial dataframe is by time


Nice to haves:

inforce files, inputs, lookups, slimming pipes, splitting the data into hot-cold dataframes, 
using disk to lower memory usages with huge datasets

"""



class Run:
    def __init__(self, start_date, start_t=0, end_t=25, year_end_month=9):
        self.start_date = start_date
        self.start_t = start_t
        self.end_t = end_t
        self.year_end_month = year_end_month
        self.func_dict = {}

    def init_df(self, input: 'DataFrame') -> 'DataFrame': 
        df = input
        df.index.names = ['t']
        self.df = df
        return df


    def process_module(self, module:str):
        funcs = ImportedFunc.get_functions(module)
        self.func_dict = dict([(f.identifier, f) for f in funcs])

        param_set = set()
        for f in funcs:
            
            param_set.add(f.identifier)
            for p in f.get_params():
                if p != 't':
                    param_set.add(p)

        missing_cols = [col for col in param_set if col not in self.df.columns]
        for col in missing_cols:
            # Currently panda's NA is signal to the system that we need to calculate this value,
            # Or alternatively error out when no related function is defined when a value is requested.
            self.df[col] = pd.NA

    
    def calculate(self):
        for t in range(0, len(self.df)):
            for col in self.df.columns:
                # visit all indicies and setting the values within underlying pandas dataframe
                # All values are memozied, so only calculated once
                # since it runs on recursive calls, dependency loops will fail (with poor error messages)
                # since it runs on recurive calls, extremely deeply nested stacks might fail.
                #   we could switch from a depth first to bredth first passes to defend against this.
                #   - use generators instead of recursion? pushing all of the first needed params onto
                #     a stack and working through that?
                #     It would be ineffeciently passing through the graph, but it would give us more 
                #     flexability in other areas of optimization. BUT, not worth it at thie point. KISS
                self.get_calc(t, col)
        
        return self.df



    def get_calc(self, t, col):
        if col == 't':
            return t
        
        if t < 0 or t >= len(self.df):
            # expected to be handled within user functions
            return 'time out of range'

        

        val = self.df.at[t, col]
        if pd.isna(val):
            print(t, col)
            if col not in self.func_dict:
                raise Exception('missing input or definition "' + col + '"')
                print(f'missing input "{col}"')
            else:
                f = self.func_dict[col]
                
                values = []
                for (pcol, pt, ptype) in f.needs(t):
                    print(pcol, pt, ptype)
                    v = self.get_calc(pt, pcol)


                    if pcol == 't':
                        values.append(t)
                    elif ptype == 'scaler':
                        values.append(v)
                    else:
                        values.append(list(self.df[pcol].values))

                value = f.fn(*values)
                self.df.at[t, col] = value
                return value
        else:
            return val




class ImportedFunc:

    types = ['scaler', 'timeseries', 'back reference', 'forward reference', 'self reference']

    def __init__(self, identifier:'str', module:'str'=None, fn:'function'=None):
        self.module = module
        self.identifier = identifier
        self.fn = fn

    def f(self):
        self.identifier

    def get_params(self):
        return self.fn.__code__.co_varnames

    def get_code(self):
        return inspect.getsource(self.fn)

    def get_type(self):
        
        truths = dict([(ty, False) for ty in ImportedFunc.types])
        code = self.get_code()
        
        params = self.get_params()
        if 't' in params:
            truths['timeseries'] = True
        else:
            truths['scaler'] = True
            return truths # <------------ return

        if self.identifier in params:
            truths['self reference'] = True


        reg_identifer_timeseries_usages = '\S*\[[^\]]*\]'
        timeseries_uses = re.findall(reg_identifer_timeseries_usages, code, re.MULTILINE)

        for use in timeseries_uses:
            if '+' in use:
                truths['forward reference'] = True
            elif '-'  in use:
                truths['back reference'] = True

        return truths


    def get_param_types(self):
        code = self.get_code()
        params = self.get_params()
        return [(p, ImportedFunc.__get_param_types__(p, code)) for p in params]
        

    @staticmethod
    def __get_param_types__(param, code):
        
        reg_identifer_timeseries_usages = param + '\[[^\]]*\]'
        timeseries_uses = re.findall(reg_identifer_timeseries_usages, code, re.MULTILINE)
        
        if len(timeseries_uses) == 0:
            return 'scaler'

        for use in timeseries_uses:
            fw = False
            bk = False
            if '+' in use:
                fw = True
            elif '-'  in use:
                bk = True

            if fw and bk:
                raise('forward and back reference on same identifier is not supported')

            if fw:
                return 'forward reference timeseries'
            elif bk:
                return 'back reference timeseries'
            else:
                return 'timeseries'

    
    def needs(self, t):
        typ = self.get_type()
        params = self.get_params()
        
        if typ == 'scaler':
            return [(p, None, 'scaler') for p in params]

        l = []
        for param, ptype in self.get_param_types():
            needed_t = 0
            if ptype == 'forward reference timeseries':
                needed_t = t+1
            elif ptype == 'back reference timeseries':
                needed_t = t-1
            elif ptype == 'timeseries':
                needed_t = t

            l.append((param, needed_t, ptype))

        return l

    @staticmethod
    def get_functions(module: str):
        rets = {}
        exec("""
import {module}

# function names as strings
fs =  [f for f in dir({module}) if '__' not in f]

# exec me to get function object"
sets = [f + ' = {module}.' + f for f in fs]
        """.format(module=module), globals(), rets)
        fs = rets['fs']
        sets = rets['sets']

        rets = {}
        exec(f"import {module}\n" + "\n".join(sets), globals(), rets)

        funcs = [rets[f] for f in fs]

        l = []
        for identifier, func in zip(fs, funcs):
            f = ImportedFunc(identifier, module, func)
            l.append(f)

        return l

