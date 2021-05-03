import pandas as pd
import numpy as np
import context
import declarative
from time import time

def test1():
    df = pd.read_csv('data/declarative_funcs.csv')

    print(df)

    run = declarative.Engine(15)
    run.init_df(df)
    run.process_module('declarative_funcs')
    df = run.df

    print(df)

    df = run.calculate()
    print(df)

    assert not df.isnull().values.any()
    assert len(df) == 15


def test_engine_init():
    engine = declarative.Engine(5)
    engine.init_df()
    engine.process_module('highlynested')
    df = engine.calculate()
    best_path = engine.best_path
    func_dict = engine.func_dict

    assert not df.isnull().values.any()
    assert len(df) == 5

    engine2 = declarative.Engine(5)
    engine2.init_df()
    engine2.func_dict = func_dict
    engine2.best_path = best_path
    engine2.process_funcs()
    df = engine.calculate()

    assert not df.isnull().values.any()
    assert len(df) == 5

def test_highlynested():
    func_dict = None
    best_path = None
    for n in range(10):
        engine = declarative.Engine(5)
        engine.init_df()
        start = time()
        if func_dict:
            engine.func_dict = func_dict
            engine.process_funcs()
        else:
            engine.process_module('highlynested')
            func_dict = engine.func_dict

        process_time = time() - start

        start = time()
        df = engine.calculate(best_path)
        best_path = engine.best_path
        calc_time = time() - start
        print(process_time, calc_time)
        print(df)

        assert not df.isnull().values.any()

def test_get_no_calculate():
    engine = declarative.Engine()
    engine.init_df()
    engine.process_module('highlynested')
    print(engine.get_calc(0, 'f49'))

def test_highlynested_timeseries():
    print('HNTS')
    func_dict = None
    best_path = None
    for n in range(10):
        engine = declarative.Engine(50 * 12)
        engine.init_df()
        start = time()
        if func_dict:
            engine.func_dict = func_dict
            engine.process_funcs()
        else:
            engine.process_module('highlynested_timeseries')
            func_dict = engine.func_dict
            
        process_time = time() - start

        start = time()
        df = engine.calculate(best_path)
        best_path = engine.best_path
        calc_time = time() - start
        print(process_time, calc_time)
        # print(df)
        print(engine.get_calc(50, 'f49'))

        assert not df.isnull().values.any()
