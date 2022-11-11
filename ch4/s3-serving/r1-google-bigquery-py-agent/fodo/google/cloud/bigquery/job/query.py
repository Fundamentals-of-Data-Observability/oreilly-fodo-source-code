#!/usr/bin/env python
# -*- encoding: utf-8 -*-
#
# Copyright 2021 Kensu Inc
#
import logging
import traceback

import google

from kensu.google.cloud.bigquery.extractor import KensuBigQuerySupport
from kensu.google.cloud.bigquery.job.bigquery_stats import compute_bigquery_stats
from kensu.google.cloud.bigquery.job.offline_parser import BqOfflineParser
from kensu.google.cloud.bigquery.job.remote_parser import BqRemoteParser
from kensu.pandas import DataFrame
from kensu.utils.helpers import report_all2all_lineage
from kensu.utils.kensu_provider import KensuProvider
import google.cloud.bigquery as bq
import google.cloud.bigquery.job as bqj

logger = logging.getLogger(__name__)


class QueryJob(bqj.QueryJob):
    ##################
    #    @('_')@     #
    ##################


    # Monkey patching with data observability the QueryJob methods:
    # - `to_dataframe`
    # - `result`
    @staticmethod
    def patch(job: bqj.QueryJob) -> bqj.QueryJob:
        return QueryJob.override_result(QueryJob.override_to_dataframe(job))

    
    # Monkey patching helper
    # - `o` is the object to be patched
    # - `f_name` is the name of the function to be patched 
    # - `f_patch` is the patching function - it takes one argument, the result of the original function
    @staticmethod
    def patcher(o, f_name, f_patch):
        # 1. Keep a reference to original non-patched method
        orig_f = getattr(o, f_name)
        # 2. Define the patching function. 
        def monkey(*args, **kwargs):
            # 3. Executing original non-patched code to get the result
            res = orig_f(*args, **kwargs)
            # 4. Patch the result
            res_patched = f_patch(res)
            # 5. Return the patched result
            return res_patched        
        # 6. Inject the documentation of the original function (important for intellisense)
        monkey.__doc__ = orig_f.__doc__
        # 7. Reassign (patch) the original method with `monkey`
        setattr(o, f_name, monkey)
        # 8. Return the patched object
        return o

    # Monkey patching `to_dataframe` to ensure that the returned pandas DataFrame is a Data Observable one (*)
    # (*) This exercise is left out from this book repository, but can be found in the `kensu` package https://github.com/kensuio-oss/kensu-py
    @staticmethod
    def override_to_dataframe(job: bqj.QueryJob) -> bqj.QueryJob:
        def observable_df(df):
            # 1. Create a data observable DataFrame 
            # (ref: package kensu - https://github.com/kensuio-oss/kensu-py/blob/5bcaa61aa25367204db6a2d09e41a615ca446a1c/kensu/pandas/data_frame.py)
            do_dataframe = df # todo 👆
            # => such DataFrame will allow the python code to track its usage in order to generate the lineage 
            # with the tables used in the QueryJob until it is written somewhere (e.g., to_csv())
            # 2. Return the data observable DataFrame
            return do_dataframe

        return QueryJob.patcher(job, "to_dataframe", observable_df)

    @staticmethod
    def override_result(job) -> bqj.QueryJob:
        def observable_job(res):
            # 1. if `job` 
            # - has a `destination` => track the lineage between the used tables and destination
            # - is a DDL, then track the creation / change of structure
            # - otherwise, probably that the data won't be used, as it is likely going to be used when to_dataframe is called (only?)
            pass

        return QueryJob.patcher(job, "result", observable_job)