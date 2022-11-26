# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import logging
from typing import Any, Dict

from superset.charts.commands.exceptions import (
    ChartDataCacheLoadError,
    ChartDataQueryFailedError,
)
from superset.commands.base import BaseCommand
from superset.common.query_context import QueryContext
from superset.exceptions import CacheLoadError

logger = logging.getLogger(__name__)


# - Kensu
from kensu.client import *
from kensu.utils.kensu_provider import KensuProvider
import urllib3
urllib3.disable_warnings()
import pandas as pd
from kensu.pandas.extractor import KensuPandasSupport
import datetime
# Kensu -

class ChartDataCommand(BaseCommand):
    _query_context: QueryContext

    # Kensu
    def generate_data_observations(self, payload):

        agent = KensuProvider().initKensu(
            process_name="superset",
            user_name="andy",
            code_location="http://localhost:8088/",
            init_context=True,
            allow_reinit=True,
            do_report=True,
            project_name="jaffle_shop",
            pandas_support=True,
            sklearn_support=False,
            bigquery_support=False,
            get_explicit_code_version_fn=lambda: "1.0.0",
            environment="dev"
        )

        def to_kensu(dso):
            ds = DataSource(name=dso["name"], format=dso["format"], categories=["logical::"+dso["name"]], pk=DataSourcePK(location=dso["location"], physical_location_ref=agent.UNKNOWN_PHYSICAL_LOCATION.to_ref()))
            sc = Schema(name = "schema", pk = SchemaPK(data_source_ref=ds.to_ref(), fields=[FieldDef(name=s["name"], field_type=s["field_type"], nullable=True) for s in dso["schema"]]))
            metrics = DataStats(pk = DataStatsPK(schema_ref=sc.to_ref(), lineage_run_ref=LineageRunRef(by_guid="fake")), stats = dso["metrics"])if "metrics" in dso else None
            ds._report()
            logging.info(sc)
            sc._report()
            return ds, sc, metrics

        in_ds = {
            "name": f"""{self._query_context.datasource.database.data["parameters"]["database"]}.{self._query_context.datasource.name}""",
            "format": self._query_context.datasource.database.data["backend"],
            "location": f"""{self._query_context.datasource.database.data["parameters"]["database"]}.{self._query_context.datasource.name}""",
            "schema": [{"name": c.column_name, "field_type": str(c.type), "nullable": True} for c in self._query_context.datasource.columns]
        }
        first_query = payload["queries"][0] # TODO when is there several?
        out_ds = {
            "name": f"""Superset:Dashboard_{self._query_context.form_data["dashboardId"]}:slice_{self._query_context.form_data["slice_id"]}""",
            "format": "Superset:Chart",
            "location": f"""/superset/explore/?form_data={{"slice_id":{self._query_context.form_data["slice_id"]}}}""",
            "schema": [{"name": n, "field_type": str(t), "nullable": True} for n, t in zip(first_query["colnames"], first_query["coltypes"])]
        }
        out_ds["metrics"] = KensuPandasSupport().extract_stats(pd.DataFrame.from_records(first_query['data']))

        logging.info("Kensu input")
        in_triplet = to_kensu(in_ds)
        logging.info("Kensu output")
        out_triplet = to_kensu(out_ds)
        logging.info("Kensu Lineage")
        lineage = ProcessLineage(name="Superset chart", operation_logic="Ignore", pk=ProcessLineagePK(process_ref = agent.process.to_ref(), data_flow = [SchemaLineageDependencyDef(from_schema_ref=in_triplet[1].to_ref(), to_schema_ref=out_triplet[1].to_ref(), column_data_dependencies={o.name: [i.name for i in in_triplet[1].pk.fields] for o in out_triplet[1].pk.fields})]))
        lineage._report()
        logging.info("Kensu Lineage Run")
        lineage_run = LineageRun(pk=LineageRunPK(process_run_ref=agent.process_run.to_ref(), lineage_ref=lineage.to_ref(), timestamp=round(datetime.datetime.now().timestamp()*1000)))
        lineage_run._report()
        if in_triplet[2] is not None:
            in_triplet[2].pk.lineage_run_ref=lineage_run.to_ref()
            logging.info("Kensu Stats")
            in_triplet[2]._report()
        if out_triplet[2] is not None:
            out_triplet[2].pk.lineage_run_ref=lineage_run.to_ref()
            logging.info("Kensu Stats")
            out_triplet[2]._report()
                
    def __init__(self, query_context: QueryContext):
        self._query_context = query_context

    def run(self, **kwargs: Any) -> Dict[str, Any]:
        # caching is handled in query_context.get_df_payload
        # (also evals `force` property)
        cache_query_context = kwargs.get("cache", False)
        force_cached = kwargs.get("force_cached", False)
        try:
            payload = self._query_context.get_payload(
                cache_query_context=cache_query_context, force_cached=force_cached
            )
            # - Kensu
            logger.info("#########################################")
            self.generate_data_observations(payload)
            logger.info("#########################################")
            # Kensu -
        except CacheLoadError as ex:
            raise ChartDataCacheLoadError(ex.message) from ex

        # TODO: QueryContext should support SIP-40 style errors
        for query in payload["queries"]:
            if query.get("error"):
                raise ChartDataQueryFailedError(f"Error: {query['error']}")

        return_value = {
            "query_context": self._query_context,
            "queries": payload["queries"],
        }
        if cache_query_context:
            return_value.update(cache_key=payload["cache_key"])

        return return_value

    def validate(self) -> None:
        self._query_context.raise_for_access()
