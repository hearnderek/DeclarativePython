# uses the pandas dataframe to make it easier for data scientiests to work with the ETL and results.
import pandas as pd
import numpy as np

# for viewing python code from functions
import inspect

# regex
import re
import time


turn_off_progress_bar = False
watches = []

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
        self.calc_count = 0
        self.start_time = None
        self.last_update_time = None
        self.df = pd.DataFrame()
        self.__df_len__ = None

    def init_df(self, input: 'DataFrame') -> 'DataFrame': 
        df = input
        df.index.names = ['t']
        self.df = df
        return df

    def get_df_len(self):
        if self.__df_len__ is None:
            self.__df_len__ = len(self.df)
        return self.__df_len__

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
        funcs = self.func_dict
        g = Graph(funcs, self.get_df_len())

        sorted_cost_cols = list(g.fnodes.values())
        sorted_cost_cols.sort(key=lambda x: x.cost)

        self.start_time = time.time()
        self.last_update_time = time.time()
        print_progress_bar(0, 100, prefix = 'Progress:', suffix = 'Complete', length = 50)
        for t in range(0, len(self.df)):
            for col in [x.identifier for x in sorted_cost_cols]:
                # visit all indicies and setting the values within underlying pandas dataframe
                # All values are memozied, so only calculated once
                # since it runs on recursive calls, dependency loops will fail (with poor error messages)
                # since it runs on recurive calls, extremely deeply nested stacks might fail.
                #   we could switch from a depth first to bredth first passes to defend against this.
                #   - use generators instead of recursion? pushing all of the first needed params onto
                #     a stack and working through that?
                #     It would be ineffeciently passing through the graph, but it would give us more 
                #     flexability in other areas of optimization. BUT, not worth it at thie point. KISS
                #print(t, col)
                self.get_calc(t, col)
        
        seconds_elapsed = int((time.time() - self.start_time)*100)/100
        print_progress_bar(100, 100, prefix = 'Progress:', suffix = f'Complete　 {seconds_elapsed} sec', length = 50)
        return self.df


    # def bredth_calculate(self):
    #     funcs = self.func_dict
    #     g = Graph(funcs, self.get_df_len())
    #     g.print()
    #     print()

    #     sorted_cost_cols = list(g.fnodes.values())
    #     sorted_cost_cols.sort(key=lambda x: x.cost)

    #     self.start_time = time.time()
    #     self.last_update_time = time.time()
    #     print_progress_bar(0, 100, prefix = 'Progress:', suffix = 'Complete', length = 50)
        
    #     while self.df.isnull().values.any():
    #         for t in range(0, self.get_df_len()):
    #             for col in [x.identifier for x in sorted_cost_cols]:
    #                 self.get_one_calc(t, col)
        
    #     seconds_elapsed = int((time.time() - self.start_time)*100)/100
    #     print_progress_bar(100, 100, prefix = 'Progress:', suffix = f'Complete　 {seconds_elapsed} sec', length = 50)
    #     return self.df


    # def get_one_calc(self, t, col):
    #     #print('get_calc', col, t)
    #     if col == 't':
    #         return t
        
    #     if t < 0 or t >= self.get_df_len():
    #         # expected to be handled within user functions
    #         return 'time out of range'

        

    #     val = self.df.at[t, col]
    #     if pd.isna(val):
    #         if col not in self.func_dict:
    #             print(self.func_dict)
    #             raise Exception('missing input or definition "' + col + '"')
    #             print(f'missing input "{col}"')
    #         else:
    #             f = self.func_dict[col]
                
    #             values = []
    #             needs = f.needs(t)
    #             has_t = False
    #             for (pcol, pt, ptype) in needs:
                    

    #                 if pcol == 't':
    #                     values.append(t)
    #                     has_t = True
    #                 elif ptype == 'scaler':
    #                     v = self.df.at[0, pcol]
    #                     if pd.isna(v):
    #                         #print('missing', 0, pcol)
    #                         return pd.NA
    #                     values.append(v)
    #                 elif ptype == 'forward reference timeseries':
    #                     at = max(0, min(pt+1, self.get_df_len()))
    #                     v = self.df.at[at, pcol]
    #                     if pd.isna(v):
    #                         # print('--> missing', at, pcol)
    #                         return pd.NA
    #                     values.append(list(self.df[pcol].values))
    #                 elif ptype == 'back reference timeseries':
    #                     at = max(0, min(pt-1, self.get_df_len()))
    #                     v = self.df.at[at, pcol]
    #                     if pd.isna(v):
    #                         # print('<- missing', at, pcol)
    #                         return pd.NA
    #                     values.append(list(self.df[pcol].values))
    #                 elif ptype == 'timeseries':
    #                     at = max(0, min(pt, self.get_df_len()))
    #                     v = self.df.at[at, pcol]
    #                     if pd.isna(v):
    #                         # print('= missing', at, pcol)
    #                         return pd.NA
    #                     values.append(list(self.df[pcol].values))
    #                 else:
    #                     print('should not happen')

                
    #             if has_t:
                    
    #                 value = f.fn(*values)
    #                 if col in watches:
    #                     print('get_calc SET', col, values, '----', value)
    #                 self.df.at[t, col] = value
    #             else:
    #                 value = f.fn(*values)
    #                 if col in watches:
    #                     print('get_calc SET', col, t, values, '----', value)
    #                 self.df[col] = value
                

    #             self.calc_count += 1

                
                
    #             if (time.time() - self.last_update_time) > 0.25: 
    #                 # This is a pretty expensive operation
    #                 # There is a noticable slowdown if checked at every 100th of a second


    #                 completed = sum(list(self.df.count()))
    #                 # print('completed', completed)
    #                 total = self.df.shape[0] * self.df.shape[1]
    #                 # print('total', total)

    #                 seconds_elapsed = int((time.time() - self.start_time)*10)/10
    #                 print_progress_bar(completed, total, prefix = 'Progress:', suffix = f'Complete　 {seconds_elapsed} sec', length = 50)
    #                 self.last_update_time = time.time()

    #             return value
    #     else:
    #         return val

            

    def get_calc(self, t, col):
        #print('get_calc', col, t)
        if col == 't':
            return t
        
        if t < 0 or t >= self.get_df_len():
            # expected to be handled within user functions
            return 'time out of range'

        

        val = self.df.at[t, col]
        if pd.isna(val):
            if col not in self.func_dict:
                print(self.func_dict)
                raise Exception('missing input or definition "' + col + '"')
                print(f'missing input "{col}"')
            else:
                f = self.func_dict[col]
                
                values = []
                needs = f.needs(t)
                has_t = False
                for (pcol, pt, ptype) in needs:
                    

                    if pcol == 't':
                        
                        values.append(t)
                        has_t = True
                    elif ptype == 'scaler':
                        v = self.get_calc(0, pcol)
                        values.append(v)
                    elif ptype == 'forward reference timeseries':
                        v = self.get_calc(pt+1, pcol)
                        values.append(list(self.df[pcol].values))
                    elif ptype == 'back reference timeseries':
                        v = self.get_calc(pt-1, pcol)
                        values.append(list(self.df[pcol].values))
                    elif ptype == 'timeseries':
                        v = self.get_calc(pt, pcol)
                        values.append(list(self.df[pcol].values))
                    else:
                        print('should not happen')
                    
                    # else:
                    #     v = self.get_calc(pt, pcol)
                    #     values.append(list(self.df[pcol].values))

                
                if has_t:
                    
                    value = f.fn(*values)
                    if col in watches:
                        print('get_calc SET', col, values, '----', value)
                    self.df.at[t, col] = value
                else:
                    value = f.fn(*values)
                    if col in watches:
                        print('get_calc SET', col, t, values, '----', value)
                    self.df[col] = value
                

                self.calc_count += 1

                
                
                if (time.time() - self.last_update_time) > 0.25: 
                    # This is a pretty expensive operation
                    # There is a noticable slowdown if checked at every 100th of a second


                    completed = sum(list(self.df.count()))
                    # print('completed', completed)
                    total = self.df.shape[0] * self.df.shape[1]
                    # print('total', total)

                    seconds_elapsed = int((time.time() - self.start_time)*10)/10
                    print_progress_bar(completed, total, prefix = 'Progress:', suffix = f'Complete　 {seconds_elapsed} sec', length = 50)
                    self.last_update_time = time.time()

                return value
        else:
            return val

class ImportedFunc:

    types = ['scaler', 'timeseries', 'back reference', 'forward reference', 'self reference']

    def __init__(self, identifier:'str', module:'str'=None, fn:'function'=None):
        self.module = module
        self.identifier = identifier
        self.fn = fn
        self.__type__ = None
        self.__param_types__ = None
        self.__is_cumulative__ = None
        self.steps = None

    def f(self):
        self.identifier

    def get_params(self):
        return self.fn.__code__.co_varnames[0:self.fn.__code__.co_argcount]

    def get_code(self):
        return inspect.getsource(self.fn)

    def get_type(self):
        if self.__type__:
            return self.__type__

        truths = dict([(ty, False) for ty in ImportedFunc.types])
        code = self.get_code()
        
        params = self.get_params()
        if 't' in params:
            truths['timeseries'] = True
        else:
            truths['scaler'] = True
            self.__type__ = truths
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

        self.__type__ = truths
        return truths


    def get_param_types(self):
        if self.__param_types__:
            return self.__param_types__
        
        code = self.get_code()
        params = self.get_params()
        self.__param_types__ = [(p, ImportedFunc.__get_param_types__(p, code)) for p in params]
        return self.__param_types__
        

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
            if not hasattr(func, '__call__'):
                # print('ignoring ', identifier)
                continue
                
            
            f = ImportedFunc(identifier, module, func)
            l.append(f)

        return l





class Graph:
    def __init__(self, function_dict, t=1):
        self.funcs = list(function_dict.items())
        # self.funcs.sort(key=lambda x: len(x[1].get_params()))
        self.fnodes = dict([(identifier, Fnode(identifier, f)) for identifier, f in self.funcs])
        for identifier, f in self.fnodes.items():
            for param in f.f.get_params():
                if param in self.fnodes:
                    f.add_param(self.fnodes[param])

        for identifier, f in self.fnodes.items():
            f.calculate_cost(t)
            f.calculate_scost()

    
    def print(self):
        values = list(self.fnodes.values())
        values.sort(key=lambda x: x.cost)
        for f in values:
            f.print()
            print()

class Fnode:
    def __init__(self, identifier: str, f: ImportedFunc):
        self.identifier = identifier
        self.f = f
        self.cost = None
        self.scost = ""
        self.params = []
        self.cost_by_param = {}
        self.cost_no_param = ''

    def add_param(self, f:'Fnode'):
        self.params.append(f)

    def calculate_cost(self, t=1):
        """
        any params not added via add_param will be assumed to be an input
        """
        self.calculate_scost()
        self.cost = self.exec_simp_scost(t)
        return self.cost

    def calculate_scost(self):
        """
        any params not added via add_param will be assumed to be an input
        """
        if self.scost:
            return self.scost

        if "t" in self.f.get_params():
            self.scost = 't'
            self.cost_no_param = 't'
        else:
            self.scost = '1'
            self.cost_no_param = '1'

        if len(self.params) != 0:
            param_costs = []
            for f in self.params:
                if f.identifier != self.identifier:
                    fc = f.calculate_scost()
                    param_costs.append(fc)
                    self.cost_by_param.update(f.cost_by_param)
                    self.cost_by_param[f.identifier] = f.cost_no_param

            self.scost += '+' + '+'.join(param_costs) 

        return self.scost

        return s    


    def simplify_scost(self):
        """
        takes the long form equation generated by calculate_scost and simplifies it
        """
        t = 0
        one = 0
        xs = list(self.cost_by_param.values())
        xs.append(self.cost_no_param)
        
        for c in list(''.join(xs)):
            if c == 't':
                t+=1
            elif c == '1':
                one+=1

        s = ''
        if t > 0:
            s+= str(t) + '*t'
        if one > 0:
            if s != '':
                s+= ' + '
            s+= str(one)

        return s    
        

    def exec_simp_scost(self, t):
        rets = {}
        exec(f"t = {t}\nx = {self.simplify_scost()}", globals(), rets)
        return rets['x']

    def print(self):
        print(self.identifier + ":")
        print("    ", self.f.get_params())
        print("    ", len(self.f.get_params()), "total params")
        print("    ", len(self.f.get_params()) - len(self.params), "inputs")
        print("    ", len(self.params), "calculated params")
        print("    ", self.simplify_scost())
        print("    ", self.cost, "cost")
        # print("<code>")
        # print(self.f.get_code())
        # print("</code>")




# lifted from: https://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console
# Thank you Stack Overflow user Greenstick
def print_progress_bar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '=', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    if turn_off_progress_bar and iteration != total:
        return
    
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + ' ' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()
    pass
