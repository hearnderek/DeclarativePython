import context
import declarative
import random



"""
Trying to get a more real life example

We'll imagine we're a website selling software.

This will be calculated on a monthly basis

"""


def num_sales(t):
    return random.randrange(15, 25)


def sales(t, num_sales, sales_price, cost_per_sale):
    return sales_price * num_sales[t] - cost_per_sale * num_sales[t]


def cash(t, initial_cash, cash, sales, fixed_expenses):
    if t <= 0:
        return initial_cash - sales[t] - fixed_expenses
    else:
        return cash[t-1] + sales[t] - fixed_expenses


if __name__ == '__main__':
<<<<<<< HEAD
    import psutil
    import os
    import pandas as pd
    import numpy as np
    p = psutil.Process(os.getpid())

    p.nice(psutil.REALTIME_PRIORITY_CLASS)
    p.cpu_affinity([1])

    timesteps = 60# * 24 * 35 * 12
    repeat = 100000 
    df = pd.DataFrame([[100, 17, 0.99, 0]], columns=['initial_cash', 'fixed_expenses', 'sales_price', 'cost_per_sale'])
    df = pd.DataFrame(np.repeat(df.values, repeat, axis=0), columns=df.columns)
    declarative.turn_off_progress_bar = True
    ie = declarative.IterativeEngine(df, 'to_profile', timesteps, False)
    ie.calculate(1)

    # df = ie.results_to_df()
    # print(df)
    # for xs in df.values:
    #     for x in xs:
    #         assert x is not pd.NA
    # assert len(df) == timesteps * repeat
=======
    idx = pd.IndexSlice
    timesteps = 35 * 12
    repeat = 100
    df = pd.DataFrame(
        [[100, 17, 0.99, 0]],
        columns=['initial_cash', 'fixed_expenses', 'sales_price', 'cost_per_sale'])
    df = pd.DataFrame(np.repeat(df.values, repeat, axis=0), columns=df.columns)
    declarative.turn_off_progress_bar = True
    ie = declarative.IterativeEngine(df, t=timesteps, display_progressbar=False)
    ie.calculate(None)

    df = ie.results_to_df()
    # print(df.loc[idx[0, 0], 'sales'])
    # print(df.loc[idx[1, 0], 'sales'])
    print(df)
    for xs in df.values:
        for x in xs:
            assert x is not pd.NA
    assert len(df) == timesteps * repeat
>>>>>>> ae28fc13c44fea63540568af6bd3859be64aedbd

