import pandas as pd
import numpy as np
import context
import declarative


def test_missing_file():
    timesteps = 5 * 12
    d = {
        'initial_account_balance': [1200],
        'initial_salary': [65000],
        'initial_yearly_expenses': [40000],
        'initial_investments': [0],
        'average_market_growth': [0.06],
        'initial_debt': [30000],
        'debt_interest_rate': [0.0376],
        'initial_monthly_debt_payment': [600],
        'initial_year': [2013],
        'initial_month': [3]
    }
    df = pd.DataFrame(d)
    declarative.turn_off_progress_bar = True
    try:
        # we specifically want this error thrown as early as possible
        ie = declarative.IterativeEngine(df, 'non_existent_file', timesteps, True)

        # we don't want the error thrown from calculate
    except ModuleNotFoundError:
        pass  # good!




def test_parallel_runs_unneeded1():
    timesteps = 5 * 12
    d = {
        'initial_account_balance': [1200],
        'initial_salary': [65000],
        'initial_yearly_expenses': [40000],
        'initial_investments': [0],
        'average_market_growth': [0.06],
        'initial_debt': [30000],
        'debt_interest_rate': [0.0376],
        'initial_monthly_debt_payment': [600],
        'initial_year': [2013],
        'initial_month': [3]
    }
    df = pd.DataFrame(d)
    declarative.turn_off_progress_bar = True
    ie = declarative.IterativeEngine(df, 'home_economics', timesteps, True)

    # giving more processers than needed
    ie.calculate(processors=2)

    df = ie.results_to_df()
    print(df[list(ie.engine.func_dict.keys())])
    print(df[list([key for key in ie.engine.func_dict.keys() if 'debt' in key])])
    for xs in df.values:
        for x in xs:
            assert x is not pd.NA
    assert len(df) == timesteps

def test_parallel_runs_unneeded2():
    timesteps = 5 * 12
    d = {
        'initial_account_balance': [1200],
        'initial_salary': [65000],
        'initial_yearly_expenses': [40000],
        'initial_investments': [0],
        'average_market_growth': [0.06],
        'initial_debt': [30000],
        'debt_interest_rate': [0.0376],
        'initial_monthly_debt_payment': [600],
        'initial_year': [2013],
        'initial_month': [3]
    }
    df = pd.DataFrame(d)
    declarative.turn_off_progress_bar = True
    ie = declarative.IterativeEngine(df, 'home_economics', timesteps, True)

    # having iterative engine to split work acorss available cpus
    ie.calculate(processors=None)

    df = ie.results_to_df()
    print(df[list(ie.engine.func_dict.keys())])
    print(df[list([key for key in ie.engine.func_dict.keys() if 'debt' in key])])
    for xs in df.values:
        for x in xs:
            assert x is not pd.NA
    assert len(df) == timesteps