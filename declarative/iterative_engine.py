import pandas as pd
import numpy as np
import multiprocessing
from .engine import Engine
import copy
import os
import sys
from sqlalchemy import create_engine
import importlib
import inspect
from pathlib import Path


class IterativeEngine:
    """
    This is a top level engine which orchestrates other engines to work on a many rows of input

    If
    """
    def __init__(self, inputs: pd.DataFrame = None, module=None, t=1, display_progressbar=True):
        if module == None:
            # gets the module of the caller
            full_path = Path(inspect.currentframe().f_back.f_globals['__file__'])
            module_name = full_path.stem

            if 'pass module object' == 'good idea':
                # problematic -- can't pickle a module, which is used when we split for parallel runs
                spec = importlib.util.spec_from_file_location(module_name, full_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
            else:
                module = module_name

        if type(module) == str:
            loader = importlib.util.find_spec(module)
            if loader is None:
                raise ModuleNotFoundError(module)
        self.module = module
        # Convert dataframe into python dictionaries for faster iteration
        self.input_columns = list(inputs.columns)
        rows = max(len(inputs), 1)
        self.input_rows = []

        d = {}
        for col in self.input_columns:
            d[col] = list(inputs[col])

        for i in range(rows):
            rd = {}
            for col in self.input_columns:
                rd[col] = [d[col][i]]
            self.input_rows.append(rd)

        self.results = {}
        self.engine = Engine(t, display_progressbar=display_progressbar)

        self.__df_results = None

    def calculate(self, processors=1, optimization=None):
        """ Will use max processors unless told otherwise """
        if processors is None:
            processors = max(1, int(multiprocessing.cpu_count() / 2))
        if processors == 1 or len(self.input_rows) == 1:
            i = 0
            for input in self.input_rows:
                self.engine.initialize(input, self.module)
                if optimization is not None:
                    self.results[i] = self.engine.calculate(optimization=optimization)
                elif len(self.input_rows) <= 2:
                    # Don't bother with any time saving calculations
                    self.results[i] = self.engine.calculate(optimization=5)
                else:
                    self.results[i] = self.engine.calculate()
                i += 1
                # print(gc.get_count())
        else:
            # TODO:
            #   - if cannot divide evenly rows at the end will be missed.
            #   - Results are completely lost.
            #   - Memory hog, Need to offload results to disk.

            n = int(len(self.input_rows) / processors)

            splits = split_list(self.input_rows, processors)
            num_splits = len(splits)
            jobs = [None] * num_splits
            dbs = [f'{self.module}{i}.sqlite' for i in range(num_splits)]
            for i in range(num_splits):
                newself = copy.deepcopy(self)
                newself.input_rows = splits[i]
                jobs[i] = multiprocessing.Process(target=newself.calculate_subset, args=(dbs[i], f'{self.module}{i}'))
                jobs[i].start()

            for job in jobs:
                job.join()

            i = 0
            for db in dbs:
                alch = create_engine(f'sqlite:///{db}', echo=False)

                sqlite_table = f'{self.module}{i}'

                with alch.connect() as sqlite_conn:
                    df = pd.read_sql_table(sqlite_table, sqlite_conn)
                    if self.__df_results is None:
                        self.__df_results = df
                    else:
                        self.__df_results = pd.concat([self.__df_results, df])

                os.remove(db)
                print(f'loaded in {i} tables')
                i += 1

            self.__df_results = self.__df_results.set_index(['result_id', 't'])

    def calculate_subset(self, dbname='db.sqlite', table=None):
        i = 0
        for input in self.input_rows:
            self.engine.initialize(input, self.module)
            if optimization is not None:
                self.results[i] = self.engine.calculate(optimization=optimization)
            elif len(self.input_rows) <= 2:
                # Don't bother with any time saving calculations
                self.results[i] = self.engine.calculate(optimization=5)
            else:
                self.results[i] = self.engine.calculate()
            i += 1

        df = self.results_to_df()
        alch = create_engine(f'sqlite:///{dbname}', echo=False)

        sqlite_table = self.module if table is None else table

        print(f'OPEN -- sqlite:///{dbname}')
        with alch.connect() as sqlite_conn:
            df.to_sql(sqlite_table, sqlite_conn, if_exists='replace')
        # print(df)

    def df_columns(self):
        """
        Generates the columns for our results to be put into a pandas' dataframe
        """
        if not self.results:
            return []

        fst = self.results[0]
        columns = ['result_id']
        columns.extend(fst.keys())

        return columns

    def results_to_df(self):
        if self.__df_results is not None:
            return self.__df_results
        """
        Put all calculated results into a pandas' dataframe.
        result_id and t will serve as our two indexes.
        """
        df_columns = self.df_columns()
        d = dict([(col, []) for col in df_columns])

        for i, result in self.results.items():

            d['result_id'].extend([i for t in result['t']])

            for col, xs in result.items():
                d[col].extend(xs)

        df = pd.DataFrame.from_dict(d, orient='columns')
        df = df.set_index(['result_id', 't'])
        return df


def split_list(xs: 'list', chunks: int) -> 'list':

    length = len(xs)
    len_over_chunks = length / chunks
    splits = [xs[int(len_over_chunks * i) : int(len_over_chunks * (i+1))] for i in range(chunks)]
    splits = [split for split in splits if len(split) > 0]
    return splits


