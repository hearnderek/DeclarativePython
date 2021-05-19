from numpy.core.numeric import full
import pandas as pd
import numpy as np
import multiprocessing
from multiprocessing.queues import Queue
from .engine import Engine
import copy
import os
import sys
import importlib
import inspect
from pathlib import Path


class EngineJob:
    def __init__(self, id, process, queue):
        self.id = id
        self.process = process
        self.queue = queue


class IterativeEngine:
    """
    This is a top level engine which orchestrates other engines to work on a many rows of input
    """

    @staticmethod
    def Run(module: str = None, inputs: pd.DataFrame = None,
            t: int = 1, display_progressbar=False,
            processors: int = 1, optimization: int = 5,
            return_dataframe: bool = True):
        """
        Run your declarative script with a one-liner
        """
        if module == None:
            # gets the module of the caller
            caller_function_script_path = inspect.currentframe().f_back.f_globals['__file__']
            module = Path(caller_function_script_path).stem

        ie = IterativeEngine(inputs, module, t, display_progressbar)
        ie.calculate(processors, optimization)
        if return_dataframe:
            return ie.results_to_df()
        else:
            return ie

    def __init__(self, inputs: pd.DataFrame = None, module=None, t=1, display_progressbar=True):
        if module == None:
            # gets the module of the caller
            full_path = Path(
                inspect.currentframe().f_back.f_globals['__file__'])
            print(inspect.currentframe().f_back)
            module_name = full_path.stem
            print('module_name', module_name)

            if 'pass module object' == 'good idea':
                # problematic -- can't pickle a module, which is used when we split for parallel runs
                spec = importlib.util.spec_from_file_location(
                    module_name, full_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
            else:
                module = module_name

        if inputs is None:
            inputs = pd.DataFrame()

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
            results = {}
            for input in self.input_rows:
                self.engine.initialize(input, self.module)
                if optimization is not None:
                    results[i] = self.engine.calculate(
                        optimization=optimization)
                elif len(self.input_rows) <= 2:
                    # Don't bother with any time saving calculations
                    results[i] = self.engine.calculate(optimization=5)
                else:
                    results[i] = self.engine.calculate()
                i += 1

            d = dict([(col, [])
                     for col in list(results[0].keys()) + ['result_id']])

            for i, result in results.items():

                d['result_id'].extend([i for t in result['t']])

                for col, xs in result.items():
                    d[col].extend(xs)

            self.results = d

        else:
            # TODO:
            #   - if cannot divide evenly rows at the end will be missed.
            #   - Results are completely lost.
            #   - Memory hog, Need to offload results to disk.

            n = int(len(self.input_rows) / processors)

            splits = split_list(self.input_rows, processors)
            num_splits = len(splits)
            engine_jobs = []
            for i in range(num_splits):
                newself = copy.deepcopy(self)
                newself.input_rows = splits[i]

                queue = Queue(ctx=multiprocessing.get_context())
                process = multiprocessing.Process(
                    target=newself.calculate_subset, args=(queue, optimization))
                process.start()

                engine_jobs.append(EngineJob(i, process, queue))

            return_dicts = []
            for engine_job in engine_jobs:
                # return_dict = {'results': engine_job.queue.get(), 'result_id': engine_job.id}
                return_dict = engine_job.queue.get()
                return_dicts.append(return_dict)
                engine_job.process.join()

            i = 0

            d = dict([(col, []) for col in list(return_dicts[0].keys())])

            for result in return_dicts:
                for col, xs in result.items():
                    d[col].extend(xs)
            self.results = d

    def calculate_subset(self, queue, optimization=5):
        i = 0
        results = {}
        for input in self.input_rows:
            self.engine.initialize(input, self.module)
            if optimization is not None:
                results[i] = self.engine.calculate(optimization=optimization)
            elif len(self.input_rows) <= 2:
                # Don't bother with any time saving calculations
                results[i] = self.engine.calculate(optimization=5)
            else:
                results[i] = self.engine.calculate()
            i += 1
            # print(gc.get_count())

        d = dict([(col, [])
                 for col in list(results[0].keys()) + ['result_id']])

        for i, result in results.items():

            d['result_id'].extend([i for t in result['t']])

            for col, xs in result.items():
                d[col].extend(xs)

        queue.put(d)

    def df_columns(self):
        """
        Generates the columns for our results to be put into a pandas' dataframe
        """
        if not self.results:
            return []

        return self.results.keys()

    def results_to_df(self):
        if self.__df_results is not None:
            return self.__df_results
        """
        Put all calculated results into a pandas' dataframe.
        result_id and t will serve as our two indexes.
        """
        df_columns = self.df_columns()

        df = pd.DataFrame.from_dict(self.results, orient='columns')
        if 'result_id' not in df_columns:
            df['result_id'] = 1

        df = df.set_index(['result_id', 't'])
        return df


def split_list(xs: 'list', chunks: int) -> 'list':

    length = len(xs)
    len_over_chunks = length / chunks
    splits = [xs[int(len_over_chunks * i): int(len_over_chunks * (i+1))]
              for i in range(chunks)]
    splits = [split for split in splits if len(split) > 0]
    return splits
