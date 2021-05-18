"""
Testing engine.py without the assistance of iterative_engine
"""

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

    run = declarative.Engine(15, input=d, module='declarative_funcs')
    df = run.results

    print(df)

    df = run.calculate(optimization=5)
    print(df)

    for xs in df.values():
        for x in xs:
            assert x is not pd.NA

    print(df)
    assert len(df['t']) == 15


def test_engine_init():
    engine = declarative.Engine(5)

    engine.initialize(module='highlynested')
    df = engine.calculate()

    for xs in df.values():
        for x in xs:
            assert x is not pd.NA
    assert len(df['t']) == 5

    engine.initialize(module='highlynested')
    df = engine.calculate()

    for xs in df.values():
        for x in xs:
            assert x is not pd.NA
    assert len(df['t']) == 5

def test_highlynested():
    engine = declarative.Engine(35 * 12)
    for n in range(10):
        engine.initialize(module='highlynested')
        d = engine.calculate()

        for xs in d.values():
            for x in xs:
                assert x is not pd.NA

def test_get_no_calculate():
    engine = declarative.Engine()
    engine.initialize(module='highlynested')
    x = engine.get_calc(0, 'f49')
    assert x is not pd.NA

def test_highlynested_timeseries():
    print('HNTS')
    timesteps = 35 * 12
    engine = declarative.Engine(timesteps)
    for n in range(10):
        engine.initialize(module='highlynested_timeseries')
        d = engine.calculate()

        print(engine.get_calc(50, 'f49'))

        for xs in d.values():
            for x in xs:
                assert x is not pd.NA

    assert len(d['t']) == timesteps