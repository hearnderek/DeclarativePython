import re
import inspect


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