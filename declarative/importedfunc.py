import re
import inspect

T = -1
SCALER = 0
TIMESERIES = 1
FORWARD_REFERENCE_TIMESERIES = 2
BACK_REFERENCE_TIMESERIES = 3


class FastTupe:
    def __init__(self, pcol, ptype):
        self.pcol = pcol
        self.ptype = ptype


class ImportedFunc:
    """
    Representation of a function loaded into our dependency calculation engine
    """

    types = ['scaler', 'timeseries', 'back reference', 'forward reference', 'self reference']

    def __init__(self, identifier: 'str', module=None, fn: 'function' = None):
        self.module = module
        self.identifier = identifier
        self.fn = fn
        self.__type__ = None
        self.__param_types__ = None
        self.__is_cumulative__ = None
        self.steps = None
        self._needs = {}
        self._needs_s = {}
        self._needs_len = None
        self._has_t = False
        self._params = []

    def get_params(self):
        if self._params:
            return self._params

        # gets all variables then only takes the first ones because they are always arguments
        self._params = self.fn.__code__.co_varnames[0:self.fn.__code__.co_argcount]
        return self._params

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
            return truths  # <------------ return

        if self.identifier in params:
            truths['self reference'] = True

        reg_identifer_timeseries_usages = r'\S*\[[^\]]*\]'
        timeseries_uses = re.findall(reg_identifer_timeseries_usages, code, re.MULTILINE)

        for use in timeseries_uses:
            if '+' in use:
                truths['forward reference'] = True
            elif '-' in use:
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

        reg_identifer_timeseries_usages = param + r'\[t[^\]]*\]'
        timeseries_uses = re.findall(reg_identifer_timeseries_usages, code, re.MULTILINE)

        if len(timeseries_uses) == 0:
            return SCALER

        for use in timeseries_uses:
            fw = False
            bk = False
            if '+' in use:
                fw = True
            elif '-' in use:
                bk = True

            if fw and bk:
                raise ('forward and back reference on same identifier is not supported')

            if fw:
                return FORWARD_REFERENCE_TIMESERIES
            elif bk:
                return BACK_REFERENCE_TIMESERIES
            else:
                return TIMESERIES

    def needs(self, t):

        if t in self._needs:
            return self._needs[t]

        typ = self.get_type()
        params = self.get_params()

        if typ == 'scaler':
            return [(p, None, SCALER) for p in params]

        ptypes = self.get_param_types()
        ls = [None for x in ptypes]
        i = 0
        for param, ptype in ptypes:
            needed_t = 0
            if param == 't':
                needed_t = None
                ptype = T
                self._has_t = True
            elif ptype == FORWARD_REFERENCE_TIMESERIES:
                needed_t = t + 1
            elif ptype == BACK_REFERENCE_TIMESERIES:
                needed_t = t - 1
            elif ptype == TIMESERIES:
                needed_t = t

            ls[i] = (param, needed_t, ptype)
            i += 1

        self._needs[t] = ls
        self._needs_s[t] = [(a, c) for (a, b, c) in ls]
        self._needs_len = i
        return ls

    @staticmethod
    def get_functions(module):
        if type(module) == str:
            return ImportedFunc.get_functions_from_str(module)
        elif type(module) == type(re):
            return ImportedFunc.get_function_from_module(module)
        else:
            raise TypeError("get_functions only accepts str and module types as input")

    @staticmethod
    def get_function_from_module(module: 'module'):
        return [ImportedFunc(id, module, f) for id, f in getmembers(inspect) if isfunction(f) and not id.startswith('_')]

    @staticmethod
    def get_functions_from_str(module: str):
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
from inspect import getmembers, isfunction
functions_list = getmembers({module}, isfunction)

# dir({module})
# function names as strings
fs =  [f for f in functions_list if '__' not in f[0]]
        """.format(module=module), globals(), rets)

        funcs = rets['fs']

        l = []
        for identifier, func in funcs:
            # if not hasattr(func, '__call__'):
            #     # print('ignoring ', identifier)
            #     continue

            f = ImportedFunc(identifier, module, func)
            l.append(f)

        return l
