import context
import declarative
import pandas as pd
import random
import time

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

    repeat = 100
    df = pd.DataFrame([[100, 17, 0.99, 0]], columns=['initial_cash', 'fixed_expenses', 'sales_price', 'cost_per_sale'])
    df = pd.DataFrame(pd.np.repeat(df.values, 100, axis=0), columns=df.columns)
    ie = declarative.IterativeEngine(df, 'test_profile', 12)
    ie.calculate()

    print(ie.results_to_df())