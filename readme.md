# Declarative Python

A calculation engine for automatically stitching together functions, and walking the dependency tree.

[This python file](https://github.com/hearnderek/DeclarativePython/blob/master/tests/home_economics.py) is the best example for seeing how this can be used to simplify your calculations.

Two major use cases are:
  1. Letting you focus on the individual functions and not the structure of the program.
  2. Writing timeseries projection models which has highly interconnected logic would be a pain to try and structure in a standard program.



# How To Use

## Install package
``` bash
python -m pip install git+https://github.com/hearnderek/DeclarativePython.
```
Oh yeah. I know. But I want to be able to use this on my remote system and not have to deal with pypi just yet.

## Basic usage

``` python
import declarative

def f() -> str:
    print('f')
    return 'hello'
    
def g(f: str) -> str:
    print('g')
    return f + ' world'
    
def output(f: str, g: str)
    print(f)
    print(g)

if __name__ == '__main__':
    declarative.Run()
```

``` sh
~$ python hello_declarative.py
f
g
hello
hello world
~$ 
```

### Okay, what just happened? 
In the above example we have three functions. f and g return values, and g uses the value return by f. Since that is obvious by the name of the parameter being exactly the same as the function, this package -- declarative -- does all of the plumming work to make that happen. In the third function output, we take f and g and output their return values. Our functions were executed in order of f -> g -> output.

You may have noticed when looking at the output that every function is only executed once. Every function output is memozied, or in otherwords saved in memory for later use. This makes sure your code runs efficiently without any effort.

## Forward Projection Calculations


``` python
import declarative

def count_up(t, count_up):
    if t == 0:
        return 0
    else:
        return count_up[t-1] + 1

if __name__ == '__main__':
    df = declarative.Run(t=10)
    print(df)
```

``` bash
~$ python forward_projection.py
             count_up
result_id t
0         0         0
          1         1
          2         2
          3         3
          4         4
          5         5
          6         6
          7         7
          8         8
          9         9
~$  
```

### Woah woah woah. What?
t is a special parameter with this system that tells our engine that you are doing calculations with distinct timesteps. You tell the Run function how many time steps you use, and within your functions you can calculate forward through 0..n. This type of programming is super common within EXCEL. "Using the result of the above cell do a calculation" You can now easily convert those excel calculations into highly similar code. Let the declarative package handle the loops, you handle the logic.

For the data savy you may have noticed the pandas DataFrame was returned by the Run function. You can write out your projections standard python then do your analysis in pandas.

## Cool, but I still want normal functions

``` python
import declarative

@declarative.ignore
def print_helper(s):
    return(s + ' world')

def f() -> str:
    print_helper('hello')

if __name__ == '__main__':
    declarative.Run()
```
``` bash
~$ python ignoreme.py
hello world
~$
```

## Do IO functions block everything else?
### Naw, I got you
``` python
import declarative
import time

@declarative.io_bound
def slow_one():
    time.sleep(1)
    return 1

@declarative.io_bound
def slow_two():
    time.sleep(1)
    return 2
    
@declarative.io_bound
def slow_three(slow_one, slow_two):
    time.sleep(1)
    return slow_one + slow_two

def output(slow_one, slow_two, slow_three):
    print(slow_one + slow_two + slow_three

if __name__ == '__main__':
    # takes 2 seconds not 3
    declarative.Run()
```
``` bash
~$ python slow_io.py
6
~$
```

# Warnings

- I am throughly abusing python within this package. Use at your own risk.
- I have not implemented any garbage collection, so all function results must be able to fit in memory.
- You can only reliably use [t] [t-1] amd [t+1] when accessing values being passed around in projections. (sorry)
