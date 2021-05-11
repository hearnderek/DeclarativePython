import context
import declarative

def etl():
    return ['value', 'hello world']

def valuation(etl):
    assert len(etl) == 2
    return etl[0]

def f():
    return 5

def g(f):

    return f * 2


def h(input_value):
    return input_value ** 2

def print_g(g):
    print(f'g:{g}')
    return f'g:{g}'

def print_f_after_g(f, print_g):
    print(f'f: {f}')
    return f'f: {f}'

if __name__ == '__main__':
    import pandas as pd
    d = {
        'input_value': [3.14, 3.22]
    }
    ie = declarative.IterativeEngine(pd.DataFrame(d), 'simple_example', display_progressbar=False)
    ie.calculate()
    print(ie.results_to_df())