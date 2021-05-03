import pandas as pd
import numpy as np
import context
import declarative
from time import time

def test1():
    df = pd.read_csv('data/declarative_funcs.csv')

    print(df)

    run = declarative.Engine('2021-04-26')
    run.init_df(df)
    run.process_module('declarative_funcs')
    df = run.df

    print(df)

    print(run.calculate())

def test_highlynested():
    func_dict = None
    best_path = None
    for n in range(100):
        engine = declarative.Engine('', 350 * 12)
        start = time()
        if func_dict:
            engine.func_dict = func_dict
        else:
            engine.process_module('highlynested')
            func_dict = engine.func_dict
        process_time = time() - start

        start = time()
        df = engine.calculate(best_path)
        best_path = engine.best_path
        calc_time = time() - start
        print(process_time, calc_time)
