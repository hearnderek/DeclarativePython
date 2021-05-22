def io_bound(func):
    func.is_io_bound = True

def is_io_bound(func) -> bool:
    if not hasattr(func, 'is_io_bound'):
        return False
    return func.is_io_bound

def ignore(func):
    func.is_ignored = True

def is_ignored(func) -> bool:
    if not hasattr(func, 'is_ignored'):
        return False
    return func.is_ignored

def is_targeted(func) -> bool:
    return not is_ignored(func)
