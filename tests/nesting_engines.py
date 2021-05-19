import pandas as pd
import numpy as np
import context
import declarative

def timesteps():
    return 12 * 15

def inputs():
    dupe = 2
    d = {
        'initial_account_balance': [1200] * dupe,
        'initial_salary': [45000] * dupe,
        'initial_yearly_expenses': [40000] * dupe,
        'initial_investments': [0] * dupe,
        'average_market_growth': [0.05, 0.06],
        'initial_debt': [30000] * dupe,
        'debt_interest_rate': [0.0376] * dupe,
        'initial_monthly_debt_payment': [600] * dupe,
        'initial_year': [2013] * dupe,
        'initial_month': [3] * dupe,
        'investment_rate': [0.10]
    }
    d = {
        'initial_account_balance': [1200],
        'initial_salary': [45000],
        'initial_yearly_expenses': [40000],
        'initial_investments': [0],
        'average_market_growth': [0.05],
        'initial_debt': [30000],
        'debt_interest_rate': [0.0376],
        'initial_monthly_debt_payment': [600],
        'initial_year': [2013],
        'initial_month': [3],
        'investment_rate': [0.10]
    }
    dupe = 4
    d = {
        'initial_account_balance': [1200] * dupe,
        'initial_salary': [45000, 45000, 50000, 50000],
        'initial_yearly_expenses': [40000, 35000, 40000, 45000],
        'initial_investments': [0] * dupe,
        'average_market_growth': [0.06] * dupe,
        'initial_debt': [30000] * dupe,
        'debt_interest_rate': [0.0376] * dupe,
        'initial_monthly_debt_payment': [600] * dupe,
        'initial_year': [2013] * dupe,
        'initial_month': [3] * dupe,
        'investment_rate': [0.10]
    }
    dupe = 2
    d = {
        'initial_account_balance': [1200] * dupe,
        'initial_salary': [45000, 50000],
        'initial_yearly_expenses': [35000, 40000],
        'initial_investments': [0] * dupe,
        'average_market_growth': [0.06] * dupe,
        'initial_debt': [30000] * dupe,
        'debt_interest_rate': [0.0376] * dupe,
        'initial_monthly_debt_payment': [600] * dupe,
        'initial_year': [2013] * dupe,
        'initial_month': [3] * dupe,
        'investment_rate': [0.10]
    }
    dupe = 3
    d = {
        'initial_account_balance': [1200] * dupe,
        'initial_salary': [40000] * dupe,
        'initial_yearly_expenses': [35000] * dupe,
        'initial_investments': [0] * dupe,
        'average_market_growth': [0.06] * dupe,
        'initial_debt': [30000] * dupe,
        'debt_interest_rate': [0.0376] * dupe,
        'initial_monthly_debt_payment': [600] * dupe,
        'initial_year': [2013] * dupe,
        'initial_month': [3] * dupe,
        'investment_rate': [0.10, 0.20, 1.00]
    }

    return pd.DataFrame(d)

def module_name():
    return 'home_economics'

def run_calculation(module_name, timesteps, inputs):
    ie = declarative.IterativeEngine(inputs, module_name, t=timesteps, display_progressbar=True)
    ie.calculate(optimization=5)
    return ie

def results(run_calculation):
    return run_calculation.results_to_df()

def year_end_net_worth(results):
    return results.loc[results['month'] == 12][['year', 'net_worth', 'account_balance', 'fixed_expenses',  'debt', 'investments', 'new_debt']]
    # return results.loc[(results.index.get_level_values('t') + 1) % 12 == 0][['year', 'month', 'net_worth']]

if __name__ == '__main__':
    from pathlib import Path
    current_file = Path(__file__).stem
    df = pd.DataFrame()
    ie = declarative.IterativeEngine(df, current_file, display_progressbar=True)
    ie.calculate(1, optimization=5)
    print(ie.results['year_end_net_worth'])
    print(ie.results['results'][0][['year', 'net_worth', 'account_balance', 'income', 'fixed_expenses', 'debt', 'investments', 'new_debt']])
    
    # print('end')

    # for key, val in ie.results.items():
    #     print(key, val)
