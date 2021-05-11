import pandas as pd
import numpy as np


import time
from .importedfunc import ImportedFunc, FORWARD_REFERENCE_TIMESERIES, BACK_REFERENCE_TIMESERIES, SCALER, TIMESERIES, T
from .graph import Graph
from .progress_bar import print_progress_bar
from warnings import warn

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
        self.display_progressbar = display_progressbar
        self.func_dict = {}
        self.calc_count = 0
        self.start_time = None
        self.last_update_time = None
        self.results = {}
        self.__df_len__ = None
        self.best_path = []
        self.build_best_path = True
        self.module = None
        self._sorted_cost_columns = []
        self._calculated = False

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

        # if self.results:
        #     # We've got a previous result saved.
        #     # lets try and reuse anything we can
        #     # This is a pretty inefficient pass through the columns.
        #     fnode: 'Fnode'
        #     self.calculated = True
        #     found_one = True
        #
        #     while found_one:
        #         not_found = []
        #         found_one = False
        #
        #         for fnode in self._sorted_cost_columns:
        #             if fnode.identifier not in not_found:
        #                 continue
        #
        #             print(fnode.identifier)
        #             f: 'ImportedFunc' = self.func_dict[fnode.identifier]
        #             col = f.identifier
        #
        #             if all(p in filled_cols for p in f.get_params()):
        #                 print(f'Filling in {col} using previous!')
        #                 filled_cols.add(col)
        #                 d[col] = self.results[col]
        #                 found_one = True
        #             else:
        #                 not_found.append(col)
        #                 self.calculated = False

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

    def process_module(self, module: str):
        """ Load in all of a file's functions into the engine. """
        funcs = ImportedFunc.get_functions(module)
        self.func_dict = dict([(f.identifier, f) for f in funcs])
        self.process_funcs()

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

        if self._calculated:
            return self.results

        if best_path is None and self.best_path:
            best_path = self.best_path
            self.build_best_path = False

        self.start_time = time.time()
        self.last_update_time = time.time()
        # if self.display_progressbar:
        #     print_progress_bar(0, 100, prefix='Progress:', suffix='Complete', length=50)

        if best_path is not None:
            # This is a 'perfect' pass through our dependency graph.
            # Note: I'm sure there are edge cases which will break this
            # each call to calculate will have all needed values available.
            self.optimized_calculate(best_path)
            # for (t, col) in best_path:
            #     self.get_calc_no_recursion(t, col)
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

        seconds_elapsed = int((time.time() - self.start_time) * 1000) / 1000
        if self.display_progressbar:
            print_progress_bar(100, 100, prefix='Progress:', suffix=f'Complete {seconds_elapsed} sec', length=50)
        self._calculated = True
        return self.results

    # @profile
    def optimized_calculate(self, best_path):
        buffer = [None] * 256
        # gc.disable()
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

        needs = f.needs(t)
        # pre-populating list to avoid use of .append()
        values = [None] * len(needs)
        has_t = False
        i = 0
        for (pcol, pt, ptype) in needs:

            if pcol == 't':
                values[i] = t
                has_t = True
            elif ptype == SCALER:
                v = self.get_calc(0, pcol)
                values[i] = v
            elif ptype == FORWARD_REFERENCE_TIMESERIES:
                v = self.get_calc(pt, pcol)
                values[i] = list(self.results[pcol])
            elif ptype == BACK_REFERENCE_TIMESERIES:
                v = self.get_calc(pt, pcol)
                values[i] = self.results[pcol]
            elif ptype == TIMESERIES:
                v = self.get_calc(pt, pcol)
                values[i] = self.results[pcol]
            else:
                print('should not happen')
            i += 1

        if has_t:
            # print(f.identifier, values)
            value = f.fn(*values)

            self.results[col][t] = value
            if self.build_best_path:
                self.best_path.append((t, col))
        else:
            value = f.fn(*values)

            for i in self.results['t']:
                self.results[col][i] = value
            if self.build_best_path:
                self.best_path.append((0, col))

        self.calc_count += 1

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

