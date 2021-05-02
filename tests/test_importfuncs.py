import pandas as pd
import context
import declarative


def import_with_deco():
    df = pd.read_csv('data/declarative_funcs.csv')
    engine = declarative.Engine('2021-04-26')
    engine.init_df(df)
    engine.process_module('declarative_funcs')

    assert 'basic' in engine.func_dict
    assert hasattr(engine.func_dict['basic'], 'ignoreme')




