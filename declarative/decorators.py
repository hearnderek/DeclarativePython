from . import function_markers

def ignore(func):
    function_markers.ignore(func)
    return func

def io_bound(func):
    function_markers.io_bound(func)
    return func