import pandas as pd
import numpy as np
import threading
import time
from warnings import warn

from .importedfunc import ImportedFunc, FORWARD_REFERENCE_TIMESERIES, BACK_REFERENCE_TIMESERIES, SCALER, TIMESERIES, T
from .graph import Graph
from .progress_bar import print_progress_bar
from .best_path import BestPath
from .function_markers import is_io_bound

"""
Need to determine my vocabulary


Inforce File:
    row based input file.
    every row of input can be run in parallel
"""


class Engine:
    """
    TODO:
    1. reuse a run object with different inputs for faster runs through a big inforce.
    2. bring in accumulation columns
    3. multiprocess
        currently inorder to run through a 112k inforce file for 35 year with monthly time steps it will take
        2 days to fully process.
        To get a 30 minute runtime, we would need to split the process across 90 CPUs. (ignoring join time)
    """

    def __init__(self, t=1, input: dict = None, module: str = None, display_progressbar=True):
        self.t = t
        self.module = None
        self.func_dict = {}
        self.results = {}
        self.__df_len__ = None
        
        self.display_progressbar = display_progressbar
        
        self.start_time = None
        self.last_update_time = None
        
        # recurive graph calculation members
        self._sorted_cost_columns = []
        self.build_best_path = True
        
        # members for building optimized calculations
        self.best_path = []
        self.bps = []
        self.flat_code = []
        self.flat_code_locals = {'self': self}
        self._calculated = False
        # flat calculation as compiled exec statment
        self.compiled = None
        # imported flat file function
        self.run = None
        

        self.initialize(input, module)

    def load_input(self, input: dict = None):
        self._calculated = False
        filled_cols = set()
        if input is not None:
            d = input
            for col in input.keys():
                v = input[col][0]
                d[col] = [v for i in range(self.t)]
                filled_cols.add(col)
        else:
            d = {}

        d['t'] = [i for i in range(self.t)]

        self.results = d

    def initialize(self, input: dict = None, module: str = None):
        self.load_input(input)

        if self.func_dict:
            self.process_funcs()
        elif module:
            self.module = module
            self.process_module(self.module)

    def init_df(self, input: dict = None) -> dict:
        """
        This is where we initialize the pandas dataframe with any inputs, if none are needed leave input as None
        """
        warn('init_df is deprecated. Please use the initialize or load_input instead')
        self.load_input(input)

        return self.results

    def process_funcs(self):
        """
        prepare engine for calculation using the already loaded functions
        """
        funcs = self.func_dict.values()
        param_set = set()
        for f in funcs:

            param_set.add(f.identifier)
            for p in f.get_params():
                if p != 't':
                    param_set.add(p)

        missing_cols = [col for col in param_set if col not in self.results]
        for col in missing_cols:
            # Currently panda's NA is signal to the system that we need to calculate this value,
            # Or alternatively error out when no related function is defined when a value is requested.
            self.results[col] = [pd.NA for x in self.results['t']]

    def sorted_columns_by_cost(self):
        """ Sorting the columns greatly reduces recursive calls of get_calc """
        if self._sorted_cost_columns:
            return self._sorted_cost_columns

        funcs = self.func_dict
        g = Graph(funcs, self.t)

        sorted_cost_cols = list(g.fnodes.values())
        sorted_cost_cols.sort(key=lambda x: x.cost)

        self._sorted_cost_columns = sorted_cost_cols
        return sorted_cost_cols

    def process_module(self, module):
        """ Load in all of a file's functions into the engine. """
        funcs = ImportedFunc.get_functions(module)
        self.func_dict = dict([(f.identifier, f) for f in funcs])
        self.process_funcs()

    def calculate(self, best_path=None, optimization=5):
        """
        using the input columns, and all of the loaded functions calculate every single value.

        Profile Results:
            get_calc is our hot function.
            The majority of our time is spent within pandas calls, unsurprisingly, getting and setting values.

            It is quite possible that we would be better off with a different data-structure for this computation,
            but then put our results back into pandas at the end.

            Another noticable bit is the pandas check isna. We might be better off using None instead for faster checks.

        """

        if self._calculated:
            return self.results

        if best_path is None and self.best_path:
            best_path = self.best_path
            self.build_best_path = False

        self.start_time = time.time()
        self.last_update_time = time.time()
        # if self.display_progressbar:
        #     print_progress_bar(0, 100, prefix='Progress:', suffix='Complete', length=50)



        if self.t == 1 and any([is_io_bound(f.fn) for f in self.func_dict.values()]):
            
            print('Now entering: PoC for conncurrency with io bound funcitons')
            
            remaining_funcs = self.func_dict.values()
            calculated_cols = set()

            while len(remaining_funcs) > 0:
                # collect all of the user function which can be run
                fs = []
                io_fs = []
                # instead of removing items from the list we're recreating the list everytime
                waiting = []
                for f in remaining_funcs:
                    needs = [col for (col, t, tt) in f.needs(0) if col not in calculated_cols]
                    if not any(needs):
                        if is_io_bound(f.fn):
                            io_fs.append(f)
                        else:
                            fs.append(f)
                    else:
                        waiting.append(f)
                remaining_funcs = waiting

                threads = []
                for f in io_fs:
                    t = threading.Thread(target=self.get_calc_no_frills, args=(0, f.identifier), daemon=True)
                    t.start()
                    threads.append(t)
                    calculated_cols.add(f.identifier)

                for t in threads:
                    t.join()

                for f in fs:
                    self.get_calc_no_frills(0, f.identifier)
                    calculated_cols.add(f.identifier)

                
                print('loop')
        elif optimization > 0 and self.optimization_prepped(optimization):
            
            # All are identical 'perfect' passes through our dependency graph.
            # They all use different approaches with different tradeoffs.
            # Note: I'm sure there are edge cases which will break this
            # - a dependency graph that changes with different inputs is an obvious 
            # each call to calculate will have all needed values available.


            if optimization == 1:
                # Limitation: 1_000_000 LoC
                # Pros: File can be reused
                # Con: Involves Writing and reading from disk for first pass
                # 0.000999
                # 0.292217
                self.flat_calc_file()
            elif optimization == 2:
                # Limitation: 1_000_000 LoC
                # Pros: No File IO
                # Cannot reuse
                self.flat_calc_in_memory()
            elif optimization == 3:
                # 0.0000997
                # 0.002
                # 0.068837
                # Con: Need to calculate python for every calculation on first pass. This is VERY slow.
                # Pro: No Limitation of LoC, and handles memory better than importlib, and compiled python is very fast.
                self.prepped_perfect_pass_get_calc()
            elif optimization == 4:
                # 0.001
                # 0.005
                # 0.174582    
                # Pro: No Limitation of Loc
                # Con: If sorted columns is an inefficient path through the graph we'll experience slowdowns
                self.perfect_pass_get_calc(best_path)
            elif optimization >= 5:
                for col in [x.identifier for x in self.sorted_columns_by_cost()[:-1]]:
                    for t in range(0, self.t):
                        self.get_calc_no_frills(t, col)
                col = self.sorted_columns_by_cost()[-1].identifier
                for t in range(0, self.t):
                    self.results[col][t] = self.get_calc_no_frills(t, col)
        else:
            # taking a calculated guess at a good path through our dependency graph
            # Guess is based on a cost function, which gets bigger the further down the chain the calls are.
            sorted_cost_cols = self.sorted_columns_by_cost()
            for col in [x.identifier for x in sorted_cost_cols]:
                for t in range(0, self.t):
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

            

        seconds_elapsed = int((time.time() - self.start_time) * 1000000) / 1000000
        if self.display_progressbar:
            print_progress_bar(100, 100, prefix='Progress:', suffix=f'Complete {seconds_elapsed} sec', length=50)
        self._calculated = True
        return self.results


    
    def optimization_prepped(self, optimization):
        if optimization == 1:
            return True if self.flat_code else False
        elif optimization == 2:
            return True if self.flat_code else False
        elif optimization == 3:
            return True if self.bps else False
        elif optimization == 4:
            return True if self.best_path else False
        elif optimization == 5:
            return True
        else:
            return False
            


    def flat_calc_file(self):
        """ 
        CHANGING STATE!
        sets all of the values within results due to pass by reference
        """

        # The underlying pass by reference lists get rebuild as new objects every time a new batch of input is received
        # for this reason we must reassign flat_code_locals result lists
        for col, values in self.results.items():
            self.flat_code_locals[f'{col}_'] = values


        if self.run is None:
            self.bps = ['Freeing up memory']


            # Creating our file
            if isinstance(self.module, str):
                tmp_file = f'tmp_{self.module}.py'
            
            with open(tmp_file, 'w') as file:
                # converting our flat calculation script to a function definition
                file.write("def run(" + ', '.join(self.flat_code_locals.keys()) + '):\n')
                for line in self.flat_code:
                    file.write(f'    {line}\n')

                self.flat_code = ['Freeing up memory']


            # loading our file into memory
            from importlib import import_module
            # dropping the ".py" extension
            runner = import_module(tmp_file[0:-3])
            # Everything is within the declared run function
            self.run = runner.run



        # run takes all user functions, pass by reference result lists
        # thanks to the magic of python argument expansion we can just use our dictionary
        self.run(*self.flat_code_locals.values())

    def flat_calc_in_memory(self):
        """ 
        CHANGING STATE!
        sets all of the values within results due to pass by reference
        """
        # The underlying pass by reference lists get rebuild as new objects every time a new batch of input is received
        # for this reason we must reassign flat_code_locals result lists
        for col, values in self.results.items():
            self.flat_code_locals[f'{col}_'] = values

        if self.compiled is None:
            self.bps = ['Freeing up memory']
            code = "\n".join(self.flat_code)
            self.flat_code = ['Freeing up memory']
            self.compiled = compile(code, '', 'exec')

        # pass by reference state and all needed inputs are passed through self.flat_code_locals
        exec(self.compiled, globals(), self.flat_code_locals)

    def prepped_perfect_pass_get_calc(self):
        """
        This is an optimized version of optimized calculate.
        """
        bp: BestPath

        gs = globals()
        just_compiled = True
        for bp in self.bps:
            if not bp.compiled:
                bp.compiled = compile(bp.code, '', 'exec')
            else:
                compiled = False
                # Only looking at first value
                break
        if just_compiled:
            self.flat_code = ['Freeing up memory']
            self.flat_code_locals = ['Freeing up memory']


        ls = {'self':self, 'f':None}
        for bp in self.bps:
            ls['f']=bp.user_func
            exec(bp.compiled, gs, ls)

    def perfect_pass_get_calc(self, best_path):
        buffer = [None] * 256
        for (t, col) in best_path:
            f = self.func_dict[col]

            # pre-populating list to avoid use of .append()
            i = 0
            for (pcol, ptype) in f._needs_s[t]:

                if ptype > SCALER:
                    buffer[i] = self.results[pcol]
                elif ptype == SCALER:
                    buffer[i] = self.results[pcol][0]
                else:  # ptype == T
                    buffer[i] = t

                i += 1

            value = f.fn(*buffer[0:i])

            if f._has_t:
                self.results[col][t] = value
            else:
                self.results[col] = [value] * self.t

    def get_calc(self, t, col):
        """
        Recursively get all the values needed to perform this calculation, save result (memoization), and return

        For scaler values use t=0
        """

        # print('get_calc', col, t)
        # print(self.results)
        if col == 't':
            return t

        if t < 0 or t >= self.t:
            # expected to be handled within user functions
            return 'time out of range'

        val = self.results[col][t]
        if val is not pd.NA:
            return val

        

        f = self.func_dict[col]

        bp = BestPath(t, col, f)

        needs = f.needs(t)
        # pre-populating list to avoid use of .append()
        values = [None] * len(needs)
        has_t = False
        i = 0


        bp_get_args = [None] * len(needs)
        bp_get_args2 = [None] * len(needs)
        for (pcol, pt, ptype) in needs:
            #print('get_calc', col, t, '--', pcol, pt, ptype)
            if pcol == 't':
                values[i] = t
                has_t = True
                bp_get_args[i] = f'{t}'
                bp_get_args2[i] = f'{t}'
            elif ptype == SCALER:
                v = self.get_calc(0, pcol)
                values[i] = v
                bp_get_args[i] = f'self.results["{pcol}"][0]'
                bp_get_args2[i] = f'{pcol}_[0]'
            elif ptype | TIMESERIES:
                v = self.get_calc(pt, pcol)
                values[i] = self.results[pcol]
                bp_get_args[i] = f'self.results["{pcol}"]'
                bp_get_args2[i] = f'{pcol}_'
            elif ptype | FORWARD_REFERENCE_TIMESERIES:
                v = self.get_calc(pt, pcol)
                values[i] = list(self.results[pcol])
                bp_get_args[i] = f'self.results["{pcol}"]'
                bp_get_args2[i] = f'{pcol}_'
            elif ptype | BACK_REFERENCE_TIMESERIES:
                v = self.get_calc(pt, pcol)
                values[i] = self.results[pcol]
                bp_get_args[i] = f'self.results["{pcol}"]'
                bp_get_args2[i] = f'{pcol}_'
            
            else:
                print('should not happen')
            i += 1

        bp_code_getter = 'f.fn(' + ', '.join(bp_get_args) + ')'
        flat_code_col2 = f'{col}(' + ', '.join(bp_get_args2) + ')'
        
        value = f.fn(*values)

        # pd.isna fails if value happens to be a list
        if isinstance(value, type(pd.NA)):
            # for some reason the calculation involved an NA value.
            # We'll try again later.
            return value

        # Generating a faster tomorrow
        if has_t:
            bp_code_setter = f'self.results["{col}"][{t}] = '
            bp.code = bp_code_setter + bp_code_getter

            flat_code_setter2 = f'{col}_[{t}] = '
            self.flat_code.append(flat_code_setter2 + flat_code_col2)
            self.results[col][t] = value
            if self.build_best_path:
                self.best_path.append((t, col))
        else:
            bp_code_setter = f'self.results["{col}"] = '
            bp.code = bp_code_setter + "[" + bp_code_getter + f"] * {self.t}"
            
            flat_code_setter2 = f'{col}_[0] = '
            self.flat_code.append(flat_code_setter2 + flat_code_col2)
            self.flat_code.append(f'for i in range(1,{self.t}):')
            self.flat_code.append(f'    {col}_[i] = {col}_[0]')
            
            for i in self.results['t']:
                self.results[col][i] = value
            if self.build_best_path:
                self.best_path.append((0, col))

        self.flat_code_locals[col] = f.fn
        self.bps.append(bp)

        if not self.last_update_time:
            self.last_update_time = time.time()
        if (time.time() - self.last_update_time) > 1.0:
            # This is a pretty expensive operation
            # There is a noticeable slowdown if checked at every 100th of a second

            completed = 0
            for xs in self.results.values():
                for x in xs:
                    if x is not pd.NA:
                        completed += 1

            # print('completed', completed)
            total = (len(self.results.keys()) - 1) * len(self.results['t'])
            # print('total', total)

            seconds_elapsed = int((time.time() - self.start_time) * 10) / 10
            if self.display_progressbar:
                print_progress_bar(completed, total, prefix='Progress:', suffix=f'Complete　 {seconds_elapsed} sec',
                                   length=50)
            self.last_update_time = time.time()

        return value

    def get_calc_no_frills(self, t, col):
        """
        Recursively get all the values needed to perform this calculation, save result (memoization), and return

        For scaler values use t=0
        """
        if col == 't':
            return t

        if t < 0 or t >= self.t:
            # expected to be handled within user functions
            return 'time out of range'


        val = self.results[col][t]
        if val is not pd.NA:
            return val
        #print('get_calc_no_frills', col, t)

        f = self.func_dict[col]
        needs = f.needs(t)
        # pre-populating list to avoid use of .append()
        args = [None] * len(needs)
        has_t = False
        i = 0
        for (pcol, pt, ptype) in needs:
            
            if pcol == 't':
                args[i] = t
                has_t = True
            elif ptype == SCALER:
                # sets results at the same time
                args[i] = self.get_calc_no_frills(0, pcol)
            elif ptype > SCALER:
                v = self.get_calc_no_frills(pt, pcol)
                args[i] = self.results[pcol]
            else:
                print('should not happen')
            i += 1
        # print('get_calc_no_frills', col, t, args)
        value = f.fn(*args)
        
        # pd.isna fails if value happens to be a list
        if isinstance(value, type(pd.NA)):
            # for some reason the calculation involved an NA value.
            # We'll try again later.
            print('WARNING: returned NA')

            return value

        if has_t:
            self.results[col][t] = value
        else:
            for i in self.results['t']:
                self.results[col][i] = value
            
         
        return value

