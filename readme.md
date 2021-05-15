# Declarative Python

A calculation engine for automatically stitching together functions, and walking the dependency tree.

[This python file](https://github.com/hearnderek/DeclarativePython/blob/master/tests/home_economics.py) is the best example for seeing how this can be used to simplify your calculations.

Two major use cases are:
  1. Letting you focus on the individual functions and not the structure of the program.
  2. Writing timeseries projection models which has highly interconnected logic would be a pain to try and structure in a standard program.


## Warning

I am throughly abusing python in an attempt to run millions of user functions blazingly fast.
1. I am importing python files in dynamically and getting functional with their internals
2. I am building massive python files and loading those in (16gb ram runs out at 3mil LoC generated python function)
3. I am making the massive assumption that your dependency between functions will not dynamically change between seriatum runs.
