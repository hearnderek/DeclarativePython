from re import M
import context
import declarative
import math

def net_worth(t, account_balance, investments, debt):
    return account_balance[t] + investments[t] - debt[t]

def account_balance(t, initial_account_balance, account_balance, expenses, income, new_investments, new_debt):
    if t == 0:
        return initial_account_balance
    else:
        return account_balance[t-1] + income[t] - expenses[t] - new_investments[t] + new_debt[t-1]

def new_debt(t, account_balance):
    if t == 0:
        return 0
    elif account_balance[t-1] < 0:
        #return 0
        return account_balance[t-1] * -1
    else:
        return 0


def debt(t, initial_debt, debt, monthly_debt_interest, monthly_debt_payment, new_debt):
    if t == 0:
        return initial_debt
    else:
        return debt[t-1] + monthly_debt_interest[t] - monthly_debt_payment[t] + new_debt[t]

def monthly_debt_payment(t, initial_monthly_debt_payment, monthly_debt_payment, debt):
    if t == 0:
        return 0.0
    elif t == 1:
        return max(0, min(initial_monthly_debt_payment, debt[t-1]))
    else:
        return max(0, min(monthly_debt_payment[t-1], debt[t-1]))

def monthly_debt_interest(t, debt_interest_rate, debt, monthly_debt_payment):
    """ Using continuously compounding interest """
    if t == 0:
        return 0.0
    if debt[t-1] - monthly_debt_payment[t-1] <= 0.0:
        return 0.0
    else:
        return debt[t-1] * (math.e ** (debt_interest_rate * 1/12)) - debt[t-1]

def investment_growth(t, investments, average_market_growth):
    monthly_investment_growth = average_market_growth / 12
    if t == 0:
        return 0.0
    else:
        return investments[t-1] * monthly_investment_growth

def investments(t, initial_investments, investments, new_investments, investment_growth):
    if t == 0:
        return initial_investments
    else:
        return investments[t-1] + investment_growth[t] + new_investments[t]

def new_investments(t, income, expenses, investment_rate):
    """ 
    going with an oversimplifed investment model where 10% of salary is invested into an index fund.
    This magic index fund always gains 5% value in a year
    """
    if t == 0:
        return 0.0
    else:
        return max(0, min(income[t] * investment_rate, income[t]-expenses[t]))

def fixed_expenses(t, initial_yearly_expenses, fixed_expenses):
    if t == 0:
        return initial_yearly_expenses / 12
    else:
        return fixed_expenses[t-1] * (1 + 0.02 / 12)


def expenses(t, monthly_debt_payment, fixed_expenses):
    return fixed_expenses[t] + monthly_debt_payment[t]

def income(t, monthly_salary_post_tax, bonus):
    return monthly_salary_post_tax[t] + bonus[t]

def monthly_salary_post_tax(t, monthly_salary, monthly_income_tax):
    return monthly_salary[t] - monthly_income_tax[t]

def monthly_salary(t, yearly_salary):
    return yearly_salary[t] / 12

def yearly_cumulative_income(t, year, yearly_cumulative_income, monthly_salary, bonus):
    if t == 0 or year[t-1] != year[t]:
        return monthly_salary[t] + bonus[t]
    else:
        return yearly_cumulative_income[t-1] + monthly_salary[t] + bonus[t]

def income_tax_brackets(t):
    brackets2021 = [
        (14200.000, 0.10),
        (54200.000, 0.12),
        (86350.000, 0.22),
        (164900.00, 0.24),
        (209400.00, 0.32),
        (523600.00, 0.35),
        (999999999999, 0.37)
    ]
    return brackets2021

def monthly_income_tax(t, year, yearly_cumulative_income, monthly_salary, bonus, income_tax_brackets):
    
    # TODO: select bracket by year
    brackets = income_tax_brackets[t]

    monthly_tax = 0.0
    taxed_income = 0.0 if t == 0 or year[t-1] != year[t] else yearly_cumulative_income[t-1]
    remaining = monthly_salary[t] + bonus[t]
    i = 0
    while remaining > 0:
        top, rate = brackets[i]
        if taxed_income < top:
            top_remaining = top - taxed_income
            to_tax = min(remaining, top_remaining)
            remaining -= to_tax
            monthly_tax += to_tax * rate
        i+=1

    return monthly_tax

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
        years = int((initial_month + t) / 12)
        return initial_year + years

def month(t, initial_month):
    return (t + initial_month) % 12 + 1


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

    timesteps = 12
    d = {
        'initial_account_balance': [1200],
        'initial_salary': [100000],
        'initial_yearly_expenses': [40000],
        'initial_investments': [0],
        'average_market_growth': [0.06],
        'initial_debt': [30000],
        'debt_interest_rate': [0.0376],
        'initial_monthly_debt_payment': [600],
        'initial_year': [2013],
        'initial_month': [3],
        'investment_rate': [0.10]
    }
    df = pd.DataFrame(d)
    declarative.turn_off_progress_bar = True
    ie = declarative.IterativeEngine(df, t=timesteps, display_progressbar=True)
    ie.calculate(1)

    df = ie.results_to_df()
    print(df[list(ie.engine.func_dict.keys())])
    # print(df[list([key for key in ie.engine.func_dict.keys() if 'debt' in key])])
    print(df[list([key for key in ie.engine.func_dict.keys() if 'inv' in key])])
    # print(df[list([key for key in ie.engine.func_dict.keys() if 'monthly' in key])])
    print(df[['year', 'monthly_salary', 'bonus', 'yearly_cumulative_income', 'monthly_income_tax']])
    for xs in df.values:
        for x in xs:
            assert x is not pd.NA
    assert len(df) == timesteps