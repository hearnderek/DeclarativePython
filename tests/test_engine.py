import pandas as pd
import numpy as np
import context
import declarative

def test1():
    df = pd.read_csv('data/declarative_funcs.csv')

    print(df)

    run = declarative.Engine('2021-04-26')
    run.init_df(df)
    run.process_module('data.declarative_funcs')
    df = run.df

    print(df)

    print(run.calculate())
