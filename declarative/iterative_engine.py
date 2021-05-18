import pandas as pd
import numpy as np
import multiprocessing
from multiprocessing.queues import Queue
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

    def calculate(self, processors=1, optimization=5):
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
            
            queues = [Queue(ctx=multiprocessing.get_context()) for i in range(num_splits)]
            dbs = [f'{self.module}{i}.sqlite' for i in range(num_splits)]
            for i in range(num_splits):
                queue = queues[i]
                queue.put({'result_id': i})
                newself = copy.deepcopy(self)
                newself.input_rows = splits[i]
                jobs[i] = multiprocessing.Process(target=newself.calculate_subset, args=(queue, optimization))
                jobs[i].start()


            return_dicts = [q.get() for q in queues]
            print('got from queues')


            for job in jobs:
                job.join()

            print('jobs done')

            i = 0

            
            
            # Set last dict as initial 
            for d in return_dicts[-1:]:
                r = d['results']
                self.results = d['results']
                self.results['result_id'] = [d['result_id']] * len(r['t'])
                print('result_id len', len(self.results['result_id']))
                r = d['results']
                for col, xs in r.items():
                    print('    ', col, len(xs))

            # In reverse order add results to beginning of list
            for d in return_dicts[-2::-1]:
                r = d['results']
                self.results['result_id'][0:0] = [d['result_id']] * len(r['t'])
                print('result_id len', len(self.results['result_id']))
                for col, xs in r.items():
                    print('    ', col, len(xs))
                    self.results[col][0:0] = xs
            
            print('result_id len', len(self.results['result_id']))
            

    def calculate_subset(self, queue, optimization=5):
        return_dict = queue.get()
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

        return_dict['results'] = self.engine.results
        queue.put(return_dict)

        print('subcalc done')

    def df_columns(self):
        """
        Generates the columns for our results to be put into a pandas' dataframe
        """
        if not self.results:
            return []

        return self.results.keys()

        return columns

    def results_to_df(self):
        if self.__df_results is not None:
            return self.__df_results
        """
        Put all calculated results into a pandas' dataframe.
        result_id and t will serve as our two indexes.
        """
        df_columns = self.df_columns()

        for col, xs in self.results.items():
            print(col, len(xs))

        df = pd.DataFrame.from_dict(self.results, orient='columns')
        df = df.set_index(['result_id', 't'])
        return df


def split_list(xs: 'list', chunks: int) -> 'list':

    length = len(xs)
    len_over_chunks = length / chunks
    splits = [xs[int(len_over_chunks * i) : int(len_over_chunks * (i+1))] for i in range(chunks)]
    splits = [split for split in splits if len(split) > 0]
    return splits


