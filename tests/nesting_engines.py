import pandas as pd
import numpy as np
import context
import declarative


def timesteps():
    return 12 * 15

def input_defaults():
    return{
        'initial_account_balance': [1200],
        'initial_salary': [40000],
        'initial_yearly_expenses': [35000],
        'initial_investments': [0],
        'average_market_growth': [0.06],
        'initial_debt': [10000],
        'debt_interest_rate': [0.1824],
        'initial_monthly_debt_payment': [600],
        'initial_year': [2013],
        'initial_month': [3],
        'investment_rate': [0.10]
    }

def inputs(input_defaults):
    dupe = 4
    d = {}
    for col, xs in input_defaults.items():
        d[col] = xs * dupe
    
    d['investment_rate'] = [0.0, 0.10, 0.20, 1.00]

    return pd.DataFrame(d)


def module_name():
    return 'home_economics'


def run_calculation(module_name: str, timesteps: int, inputs: 'pd.DataFrame'):
    return declarative.IterativeEngine.Run(module_name, inputs, t=timesteps, display_progressbar=True, optimization=5, return_dataframe=False)


def results(run_calculation):
    return run_calculation.results_to_df()


def year_end_net_worth(results: 'pd.DataFrame'):
    return results.loc[results['month'] == 12][['year', 'net_worth', 'account_balance', 'fixed_expenses',  'debt', 'investments', 'new_debt']]
    # return results.loc[(results.index.get_level_values('t') + 1) % 12 == 0][['year', 'month', 'net_worth']]


if __name__ == '__main__':

    ie = declarative.IterativeEngine.Run(
        display_progressbar=True, return_dataframe=False)
    print(ie.results['year_end_net_worth'])
    print(ie.results['results'][0][['year', 'net_worth', 'account_balance',
          'income', 'fixed_expenses', 'debt', 'investments', 'new_debt']])

    # print('end')

    # for key, val in ie.results.items():
    #     print(key, val)
