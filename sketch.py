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
    """
    TODO:
    1. reuse a run object with different inputs for faster runs through a big inforce.
    2. bring in accumulation columns
    3. multiprocess
        currently inorder to run through a 112k inforce file for 35 year with monthly time steps it will take
        2 days to fully process. 
        To get a 30 minute runtime, we would need to split the process across 90 CPUs. (ignoring join time)
    """

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
        self.best_path = []

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

    
    def calculate(self, best_path=None):
        """
        using the input columns, and all of the loaded functions calculate every single value.

        Profile Results:
            get_calc is our hot function.
            The majority of our time is spent within pandas calls, unsurprisingly, getting and setting values.

            It is quite possible that we would be better off with a different data-structure for this computation,
            but then put our results back into pandas at the end. 

            Another noticable bit is the pandas check isna. We might be better off using None instead for faster checks.

        """

        self.start_time = time.time()
        self.last_update_time = time.time()
        print_progress_bar(0, 100, prefix = 'Progress:', suffix = 'Complete', length = 50)
        
        if best_path is not None:
            # The best pass is something the user can pass into to speed up the calculation.
            # The easiest way to do this is by letting one pass populate self.best_path,
            # then pass that back in for repeated runs.
            for (t, col) in best_path:
                self.get_calc(t, col)
        else:
            # taking a calculated guess at a good path through our dependency graph
            # Guess is based on a cost function, which gets bigger the further down the chain the calls are.
            funcs = self.func_dict
            g = Graph(funcs, self.get_df_len())

            sorted_cost_cols = list(g.fnodes.values())
            sorted_cost_cols.sort(key=lambda x: x.cost)

            for col in [x.identifier for x in sorted_cost_cols]:
                for t in range(0, self.get_df_len()):
                    # visit all indicies and setting the values within underlying pandas dataframe
                    # All values are memozied, so only calculated once
                    # since it runs on recursive calls, dependency loops will fail (with poor error messages)
                    # since it runs on recurive calls, extremely deeply nested stacks might fail.
                    #   we could switch from a depth first to bredth first passes to defend against this.
                    #   - use generators instead of recursion? pushing all of the first needed params onto
                    #     a stack and working through that?
                    #     It would be ineffeciently passing through the graph, but it would give us more 
                    #     flexability in other areas of optimization. BUT, not worth it at thie point. KISS
                    #
                    # After an attempt to do bredth first pass, reverted back to this simple method.
                    # since we're lazy in figuring out what index is needed  
                    self.get_calc(t, col)
        
        seconds_elapsed = int((time.time() - self.start_time)*100)/100
        print_progress_bar(100, 100, prefix = 'Progress:', suffix = f'Complete　 {seconds_elapsed} sec', length = 50)
        return self.df
            

    def get_calc(self, t, col):
        """
        Recursively get all the values needed to perform this calculation, save result (memoization), and return

        For scaler values use t=0

        TODO: optimize this, for it is the bottleneck.
        - move away from string checks4
        """

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
                    self.best_path.append( (t, col) )
                else:
                    value = f.fn(*values)
                    if col in watches:
                        print('get_calc SET', col, t, values, '----', value)
                    self.df[col] = value
                    self.best_path.append( (-1, col) )
                

                self.calc_count += 1

                
                
                if (time.time() - self.last_update_time) > 1.0: 
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
    """
    Representation of a function loaded into our dependency calculation engine
    """

    types = ['scaler', 'timeseries', 'back reference', 'forward reference', 'self reference']

    def __init__(self, identifier:'str', module:'str'=None, fn:'function'=None):
        self.module = module
        self.identifier = identifier
        self.fn = fn
        self.__type__ = None
        self.__param_types__ = None
        self.__is_cumulative__ = None
        self.steps = None

    def get_params(self):
        # gets all variables then only takes the first ones because they are always arguments
        return self.fn.__code__.co_varnames[0:self.fn.__code__.co_argcount]

    def get_code(self):
        return inspect.getsource(self.fn)

    def get_type(self):
        """
        Parsing our function's code to determine how it fits within the dependency graph.
        Currently only supports t+1, t-1
        Constants will have undefined behavior, and [t] is an infiniate loop.

        types:
            scaler - same for all values of t
            timeseries - has a value for every value of t
            self reference - RECURSION but for different values of t
        """
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
        """
        parsing the user defined code to better understand timeseries index usage.

        Since code parsing of multiple functions is expensive, the result will be saved and returned on subsequent calls.
        """
        if self.__param_types__:
            return self.__param_types__
        
        code = self.get_code()
        params = self.get_params()
        self.__param_types__ = [(p, ImportedFunc.__get_param_types__(p, code)) for p in params]
        return self.__param_types__
        

    @staticmethod
    def __get_param_types__(param, code):
        """
        parsing the user defined code to better understand timeseries index usage.

        TODO: better parsing to we can calculate for any value of t what index will be accessed
            If we know this we can calculate a like optimal graph traversal.
        """
        
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
        """
        Abusing the python interpreter to do our work.

        Load in a specific module/file and return all local variables
        Then keep only the functions.
        ! HELPER FUNCTIONS:
            should not be declared within the module.
        
            generally bad:
                from mymodule import myfunction
                myfunction(...)

            use instead:
                import mymodule
                mymodule.myfunction(...)
        """

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
    """
    Using a dictionary list of ImportedFuncs, encapsulate with Fnodes, make it easier to work with our dependency graph.

    If we want to add visualization logic, it will be added here.
    """

    def __init__(self, function_dict, t=1):
        """
        function_dict:
            dictionary using function identifiers as keys, and ImportedFuncs as values

        t:
            number of timeseries steps which will be used in our calculation. 
            Used within cost calculation.
            Ignore this unless you want to see how costs increase/decrease based on number of timesteps
        """
        
        self.funcs = list(function_dict.items())
        # self.funcs.sort(key=lambda x: len(x[1].get_params()))
        self.fnodes = dict([(identifier, Fnode(identifier, f)) for identifier, f in self.funcs])
        for identifier, f in self.fnodes.items():
            for param in f.f.get_params():
                if param in self.fnodes:
                    f.add_param(self.fnodes[param])

        for identifier, f in self.fnodes.items():
            f.calculate_cost(t)


    def get_heads(self):
        """
        a head is an fnode which is dependent on no other fnodes.

        A node which is dependent on an input is still a head.
        Inputs are not included within the dependency graph so they are not heads.
        """
        return [f for identifier, f in self.fnodes.items() if f.is_head()]
        
    
    def print(self):
        values = list(self.fnodes.values())
        values.sort(key=lambda x: x.cost)
        for f in values:
            f.print()
            print()

    def total_cost(self):
        """
        Give a cost for calculating every single value.

        Can be done by making a node which takes every fnode as a parameter, then calculating the cost on that.
        """
        pass

class Fnode:
    """
    Encapsulation of our ImportedFuncs, making them propper nodes of a unidirectional dependency graph.

    We get cheeky with string manipulation since it was simple to implement, and it not being a detrement
    to speed
    """
    def __init__(self, identifier: str, f: ImportedFunc):
        self.identifier = identifier
        self.f = f
        self.cost = None
        self.scost = ""
        self.params = []
        self.cost_by_param = {}
        self.cost_no_param = ''

    def add_param(self, f:'Fnode'):
        """
        Any parameter not added in this fashion will be assumed to be input.
        """
        self.params.append(f)

    def param_names(self):
        return [f.identifier for f in self.params]

    def calculate_cost(self, t=1):
        """
        Calculate Cost of Function within Dependency Graph

        This will walk through all nodes which this function depends on.
        """
        if self.cost:
            return self.cost
        
        self.calculate_scost()
        self.cost = self.exec_simp_scost(t)
        return self.cost

    def calculate_scost(self):
        """
        Calculate Cost of Function within Dependency Graph (with strings)

        This will walk through all nodes which this function depends on.
        Since our Run object ensures every value is only calculated once (memozation)
        we want so ensure costs for multi-referenced functions are not double counted.
        We do this by storing them in a unique set and summing those equations together.
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


    def simplify_scost(self):
        """
        takes the long form equation generated by calculate_scost and simplifies it

        returns: 
            x*t + c

            ex. 5*t + 2


        Why: lots of ts and 1s added together isn't exactly readable...
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
        """
        Executing our simplified equation to get a cost

        Yes, we could parse it easily, but so can python!
        """

        rets = {}
        exec(f"t = {t}\nx = {self.simplify_scost()}", globals(), rets)
        return rets['x']

    def is_head(self):
        """
        a head is a node is a function which is dependent on no other functions.

        A node which is dependent on an input is still a head.
        Inputs are not included within the dependency graph so they are not heads.
        """
        if self.exec_simp_scost(1) == 1:
            return True
        else:
            return False

    def num_inputs(self):
        return len(self.f.get_params()) - len(self.params)

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
