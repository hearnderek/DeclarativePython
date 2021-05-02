from .importedfunc import ImportedFunc

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
