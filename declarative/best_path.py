from .importedfunc import ImportedFunc

class BestPath:
    def __init__(self, t:int, col:str, f:ImportedFunc) -> None:
        self.t = t
        self.col = col
        self.user_func = f
        self.code = ''
