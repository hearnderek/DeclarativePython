import contextlib
import declarative
import pandas as pd

def total_population_monthly():
    """ Monthly US cencus estimates 1952 - 2019 """
    df = pd.read_csv('./data/POP.csv', date_parser=True)
    df = df[['date', 'value']]
    df = df[df['date'].str.endswith('01')]
    df['year'] = pd.to_numeric(df['date'].str[0:4])
    df['month'] = pd.to_numeric(df['date'].str[5:7])
    del df['date']
    df.columns = ['population', 'year', 'month']
    return df

def population_by_age_00s():
    """
    Monthly US cencus estimates with age, sex, and race data 2000 - 2010
<<<<<<< HEAD
    85 = 85 and up
=======
>>>>>>> 04e6e4bda66d9e5a18c50eac9cfd6a73a0aa70b9
    Age 999 = all ages
    """
    df = pd.read_csv('./data/us-est00int-alldata.csv')
    df = df[['MONTH', 'YEAR', 'AGE', 'TOT_POP', 'TOT_MALE', 'TOT_FEMALE']]
    df.columns = ['month', 'year', 'age', 'population', 'male', 'female']
    return df


def individual_deaths_2005(total_population_monthly: pd.DataFrame):
    """ 
<<<<<<< HEAD
    every individual recorded US death in 2005
    Age 999 = unknown age 
    """
    df = pd.read_csv('./data/2005_deaths.csv')
    
    #df.to_csv('./data/2005_deaths.csv', index=False)
=======
    every individual recorded US death in 2005 -- 85 = 85 and up
    Age 999 = unknown age 
    """
    df = pd.read_pickle('./data/2005_data_smol.pickle')
>>>>>>> 04e6e4bda66d9e5a18c50eac9cfd6a73a0aa70b9
    df = pd.merge(df, total_population_monthly, on=['month', 'year'])

    return df

def deaths(individual_deaths_2005: pd.DataFrame, population_by_age_00s: pd.DataFrame):
    """ Combining population data with death data to get what percentage of the population died """
    deaths = individual_deaths_2005.groupby(['year', 'month', 'age']).size()
    deaths = deaths.to_frame('deaths')
    deaths.reset_index(inplace=True)
    df = pd.merge(deaths, population_by_age_00s[['year', 'age', 'population']], on=['year', 'age'])
    df['%'] = df['deaths'] / df['population']
    return df


def print_pop(total_population_monthly: pd.DataFrame):
    print(total_population_monthly)

def print_deaths(deaths):
    print(deaths)

def print_data(individual_deaths_2005):
    print(individual_deaths_2005)

def write_deaths_to_csv(deaths: pd.DataFrame):
    deaths.to_csv('./data/deaths2005.csv')

if __name__ == '__main__':
    declarative.Run(return_dataframe=False)
    
