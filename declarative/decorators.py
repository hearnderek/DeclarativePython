from . import function_markers

def ignore(func):
    function_markers.ignore(func)
    return func

def io_bound(func):
    function_markers.io_bound(func)
    return func

# just incase someone imports these functions
function_markers.ignore(ignore)
function_markers.ignore(io_bound)
