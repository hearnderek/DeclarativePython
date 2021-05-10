import context
import declarative

def net_worth(t, account_balance, investments):
    return account_balance[t] + investments[t]

def account_balance(t, initial_account_balance, account_balance, expenses, income, new_investments):
    if t == 0:
        return initial_account_balance
    else:
        return account_balance[t-1] + income[t] - expenses[t] - new_investments[t]

def investment_growth(t, investments):
    monthly_investment_growth = 0.06 / 12
    if t == 0:
        return 0.0
    else:
        return investments[t-1] * monthly_investment_growth

def investments(t, initial_investments, investments, new_investments, investment_growth):
    if t == 0:
        return initial_investments
    else:
        return investments[t-1] + investment_growth + new_investments

def new_investments(t, income, expenses):
    """ 
    going with an oversimplifed investment model where 10% of salary is invested into an index fund.
    This magic index fund always gains 5% value in a year
    """
    return max(0, min(income[t] * 0.1, income[t]-expenses[t]))


def expenses(t, initial_yearly_expenses, expenses):
    if t == 0:
        return initial_yearly_expenses / 12
    else:
        return expenses[t-1] * (1 + 0.02 / 12)

def income(t, monthly_salary, bonus):
    return monthly_salary[t] + bonus[t]

def monthly_salary(t, yearly_salary):
    return yearly_salary[t] / 12

def yearly_salary(t, initial_salary, yearly_salary, salary_increase):
    if t == 0:
        return initial_salary
    else:
        return yearly_salary[t-1] + salary_increase[t]

def salary_increase(t, yearly_salary, month):
    if month[t] == 6:
        return yearly_salary[t-1] * 0.02
    else:
        return 0.0

def year(t, initial_year, initial_month):
    if t == 0:
        return initial_year
    else:
        years = int((initial_month + t - 1) / 12)
        return initial_year + years

def month(t, initial_month):
    return (t + initial_month) % 13


def bonus(t, yearly_salary, month):
    if month[t] == 12:
        return yearly_salary[t] * 0.10
    else:
        return 0.0

if __name__ == '__main__':
    import pandas as pd
    import numpy as np
    from pathlib import Path
    current_file = Path(__file__).stem

    timesteps = 5 * 12
    df = pd.DataFrame([[1200, 0, 65000, 40000,  2013, 3]], columns=['initial_account_balance', 'initial_investments', 'initial_salary', 'initial_yearly_expenses', 'initial_year', 'initial_month'])
    declarative.turn_off_progress_bar = True
    ie = declarative.IterativeEngine(df, current_file, timesteps, True)
    ie.calculate(1)

    df = ie.results_to_df()
    print(df)
    for xs in df.values:
        for x in xs:
            assert x is not pd.NA
    assert len(df) == timesteps