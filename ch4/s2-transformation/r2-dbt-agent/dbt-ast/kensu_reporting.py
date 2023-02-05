import logging
import os
import re
from collections import defaultdict
from multiprocessing.synchronize import RLock
from dbt import flags
from typing import Dict, Hashable, List
from dbt.adapters.base.connections import BaseConnectionManager

from kensu.utils.kensu import Kensu, KensuDatasourceAndSchema
from kensu.utils.kensu_provider import KensuProvider
from kensu_postgres import pg_relation_to_kensu_table_name, pg_to_kensu_entry, report_postgres

KENSU_FACTORY_LOCK: RLock = flags.MP_CONTEXT.RLock()
KENSU_FACTORY_AGENTS: Dict[Hashable, Kensu] = {}
# TODO function to clean the agents

KENSU_CACHE_SEEDS: Dict[Hashable, List[KensuDatasourceAndSchema]] = defaultdict(list)

def current_thread_key():
    key = BaseConnectionManager.get_thread_identifier()
    return key

def get_kensu_agent():
    key = current_thread_key()
    with KENSU_FACTORY_LOCK:
        agent = KENSU_FACTORY_AGENTS.get(key)
        logging.info("🚀 Getting Kensu Client in thread " + str(key))

        return agent

def init_kensu(
        project,
        app_name,
        code_version,
        codebase_location,
        run_environment,
        user_name=None,
):
    key = current_thread_key()
    with KENSU_FACTORY_LOCK:
        if key in KENSU_FACTORY_AGENTS:
            logging.info("🚀 Kensu Client for app " + app_name + " already exist in thread " + str(key))
            agent = KENSU_FACTORY_AGENTS[key]
            if agent.process.pk.qualified_name != app_name:
                logging.info("🚀 Kensu Client for app " + app_name + " must replace existing one (assuming task for app "+agent.process.pk.qualified_name+" is done) in thread " + str(key))
            else:
                return KENSU_FACTORY_AGENTS[key]
        from kensu.utils.kensu_provider import KensuProvider
        import urllib3
        urllib3.disable_warnings()

        import sys
        log_format = '%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s'
        # TODO scope the logging conf
        logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=log_format)

        logging.info("💥 Creating new Kensu Client for " + app_name + " in thread " + str(key))

        agent = KensuProvider().initKensu(
            # p.s. commented ones are taken for conf.ini
            # kensu_ingestion_url=api_url,
            # kensu_ingestion_token=token,
            # report_to_file=True,
            # offline_file_name='kensu_events.log',
            process_name=app_name,
            user_name=user_name,
            code_location=codebase_location,
            init_context=True,
            allow_reinit=True,
            do_report=True,
            project_name=project,
            pandas_support=False,
            sklearn_support=False,
            bigquery_support=True,
            get_explicit_code_version_fn=lambda: code_version,
            environment=run_environment
        )
        KENSU_FACTORY_AGENTS[key] = agent
        return agent


def dbt_init_kensu(context, model):
    from kensu_reporting import init_kensu, str_remove_prefix
    kmodel = context['model'] or {}
    kensu_project = kmodel['package_name']

    project_version = context['source'].config.version
    code_version_from_env = os.environ.get('KSU_CODE_VERSION')
    kensu_collector = init_kensu(
        project=kensu_project,
        # e.g. model.unique_id='model.kensu_bigquery_simple.my_first_dbt_model'
        # e.g. model.name=my_first_dbt_model
        app_name='dbt :: ' + model.name,
        code_version=code_version_from_env or f"v{project_version}",
        # e.g. 'kensu_bigquery_simple://models/example/my_first_dbt_model.sql'
        codebase_location=model.file_id,
        run_environment=context['target']['name']
        # TODO: review codebase location?
        # - or should it be the project itself (kensu_bigquery_simple://), as numeric version is attached to it?
        # - or should it be Git?
        # TODO: user_name taken from conf.ini for now, do we want env var?
        # TODO: explicit run id?
    )
    return kensu_collector


def get_sensitive_fields(model):
    sensitive_fields = []
    for out_column in model.columns.values():
        col_metadata = (out_column.meta or {})
        if col_metadata.get('sensitive', False):
            sensitive_fields.append(out_column)
    return sensitive_fields


def str_remove_prefix(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix):]
    return s

# TODO => https://agate.readthedocs.io/en/1.6.1/api/table.html#agate.Table.rows
# & https://agate.readthedocs.io/en/1.6.1/api/aggregations.html
def compute_csv_stats(agate_table):
    return {"nrows": len(agate_table.rows)}

def intercept_seed_table(agate_table):
    key = current_thread_key()
    agent = get_kensu_agent()
    cleaned_name = os.path.basename(agate_table.original_abspath)
    # TODO better types => https://github.com/wireservice/agate/blob/master/agate/data_types/boolean.py
    schema = list(zip(agate_table.column_names, [x.__class__.__name__ for x in agate_table.column_types])) 
    kensuDatasourceAndSchema = KensuDatasourceAndSchema.for_path_with_opt_schema(
                                ksu=agent,
                                ds_path=agate_table.original_abspath,  
                                ds_name=cleaned_name,  
                                format='csv',
                                categories=['logical::' + f"{cleaned_name}"],
                                maybe_schema=schema,
                                f_get_stats=lambda: compute_csv_stats(agate_table)
                            ) 
    KENSU_CACHE_SEEDS[key].append(kensuDatasourceAndSchema)

def get_current_thread_seeds():
    key = current_thread_key()
    return KENSU_CACHE_SEEDS.get(key, [])

def extract_kensu_output_datasources(model_runner, k, result, model):
    for relation in model_runner._materialization_relations(result, model):
        try:
            maybe_pg_table = pg_relation_to_kensu_table_name(relation)
            ds = None
            if maybe_pg_table is not None:
                # Postgres
                k_entry = pg_to_kensu_entry(
                    kensu_inst=KensuProvider().instance(),
                    cursor=model_runner.adapter.connections.get_thread_connection().handle.cursor(),
                    tname=maybe_pg_table,
                    compute_stats=False
                )
                ds = k_entry.ksu_ds
            else:
                # BigQuery
                table_ref = model_runner.adapter.get_table_ref_from_relation(relation)
                # FIXME: should be re-written to work with multiple DB types
                # also we could possibly even skip query to DB (at least for some use-cases)
                conn = model_runner.adapter.connections.get_thread_connection()
                bq_table = conn.handle.get_table(table_ref)
                from kensu.utils.helpers import extract_ksu_ds_schema
                ds_schema = extract_ksu_ds_schema(kensu=k, orig_variable=bq_table, report=False,
                                                  register_orig_data=False)
                if ds_schema:
                    ds, schema = ds_schema
            yield ds
        except Exception as e:
            logging.warning('failure in extract_kensu_output_datasources', e)

def maybe_report_sql(conn_mngr, cursor, sql, bindings):
    logging.info("Intercept SQL: " + sql)
    status = False
    try:
        # Trying postgres first
        status = report_postgres(conn_mngr=conn_mngr, cursor=cursor, sql=sql, bindings=bindings)
        if status is None:
            # continue with other handlers => we should use the "listener/visitor" pattern with a registry
            pass
        return
    except:
        logging.exception('Got exception on main handler')
        raise

