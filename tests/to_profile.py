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
    import psutil
    import os
    import pandas as pd
    import numpy as np

    # Make our process run on a dedicated CPU
    # p = psutil.Process(os.getpid())
    # p.nice(psutil.REALTIME_PRIORITY_CLASS)
    # p.cpu_affinity([1])

    timesteps = 60 * 24 #* 35 #* 12
    repeat = 30
    # Test every optimization for single and multiprocess
    for processors in [1, 3]:
        for optimization in range(1,6):
            df = pd.DataFrame([[100, 17, 0.99, 0]], columns=['initial_cash', 'fixed_expenses', 'sales_price', 'cost_per_sale'])
            df = pd.DataFrame(np.repeat(df.values, repeat, axis=0), columns=df.columns)
            declarative.turn_off_progress_bar = True
            ie = declarative.IterativeEngine(df, 'to_profile', timesteps, True)
            ie.calculate(processors, optimization)

            df = ie.results_to_df()
            print(df)
        
            # ASSERT NO NA
            for col in df.columns:
                assert df.isna().sum()[col] == 0, f"running with {processors} processor(s) -- in optimization {optimization} -- {col} is has pd.NA values"

            # ASSERT NO MISSING ROWS
            assert len(df) == timesteps * repeat, f'running with {processors} processor(s) -- in optimization {optimization} -- {len(df)} <> {timesteps * repeat}'

