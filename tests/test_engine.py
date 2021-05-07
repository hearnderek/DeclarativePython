import pandas as pd
import numpy as np
import context
import declarative
from time import time

def test1():
    df = pd.read_csv('data/declarative_funcs.csv')

    d = {}
    for col in df.columns:
        d[col] = list(df[col])

    print(d)
    df = d

    run = declarative.Engine(15)
    run.init_df(df)
    run.process_module('declarative_funcs')
    df = run.results

    print(df)

    df = run.calculate()
    print(df)

    for xs in df.values():
        for x in xs:
            assert x is not pd.NA

    print(df)
    assert len(df['t']) == 15


def test_engine_init():
    engine = declarative.Engine(5)
    engine.init_df()
    engine.process_module('highlynested')
    df = engine.calculate()
    best_path = engine.best_path
    func_dict = engine.func_dict

    for xs in df.values():
        for x in xs:
            assert x is not pd.NA
    assert len(df['t']) == 5

    engine2 = declarative.Engine(5)
    engine2.init_df()
    engine2.func_dict = func_dict
    engine2.best_path = best_path
    engine2.process_funcs()
    df = engine.calculate()

    for xs in df.values():
        for x in xs:
            assert x is not pd.NA
    assert len(df['t']) == 5

def test_highlynested():
    func_dict = None
    best_path = None
    engine = declarative.Engine(35 * 12)
    for n in range(10):
        engine.init_df()
        start = time()
        if func_dict:
            engine.process_funcs()
            df = engine.calculate(best_path)
        else:
            engine.process_module('highlynested')
            func_dict = engine.func_dict
            df = engine.calculate(best_path)
            best_path = engine.best_path

        for xs in df.values():
            for x in xs:
                assert x is not pd.NA

def test_get_no_calculate():
    engine = declarative.Engine()
    engine.init_df()
    engine.process_module('highlynested')
    print(engine.get_calc(0, 'f49'))

def test_highlynested_timeseries():
    print('HNTS')
    func_dict = None
    best_path = None
    engine = declarative.Engine(35 * 12)
    for n in range(10):
        engine.init_df()
        start = time()
        if func_dict:
            engine.process_funcs()
            df = engine.calculate(best_path)
        else:
            engine.process_module('highlynested_timeseries')
            func_dict = engine.func_dict
            df = engine.calculate(best_path)
            best_path = engine.best_path

        # print(df)
        print(engine.get_calc(50, 'f49'))

        for xs in df.values():
            for x in xs:
                assert x is not pd.NA
