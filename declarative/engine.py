import pandas as pd
import time
from .importedfunc import ImportedFunc
from .graph import Graph

turn_off_progress_bar = False
watches = []

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
