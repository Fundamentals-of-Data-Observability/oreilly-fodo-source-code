import datetime
import logging
import urllib3
urllib3.disable_warnings()
from kensu.client import *
from kensu.utils.kensu_provider import KensuProvider
import pandas as pd
from kensu.pandas.extractor import KensuPandasSupport

def generate_data_observations(chart_data_command, chart_data_result_payload):
    import os
    if "KSU_CONF_FILE" not in os.environ:
        os.environ["KSU_CONF_FILE"] = "conf.ini"

    agent = KensuProvider().initKensu(init_context=True,allow_reinit=True,do_report=True,get_explicit_code_version_fn=lambda: "1.0.0")

    def to_kensu(dso):
        ds = DataSource(name=dso["name"], format=dso["format"], categories=["logical::"+dso["name"]], 
                        pk=DataSourcePK(location=dso["location"], physical_location_ref=agent.UNKNOWN_PHYSICAL_LOCATION.to_ref()))
        sc = Schema(name = "schema", pk = SchemaPK(data_source_ref=ds.to_ref(), 
                    fields=[FieldDef(name=s["name"], field_type=s["field_type"], nullable=True) for s in dso["schema"]]))
        metrics = DataStats(pk = DataStatsPK(schema_ref=sc.to_ref(), lineage_run_ref=LineageRunRef(by_guid="fake")), 
                            stats = dso["metrics"]) if "metrics" in dso else None
        ds._report()
        logging.info(sc)
        sc._report()
        return ds, sc, metrics

    qc = chart_data_command._query_context
    in_ds = {
        "name": f"""{qc.datasource.database.data["parameters"]["database"]}.{qc.datasource.name}""",
        "format": qc.datasource.database.data["backend"],
        "location": f"""{qc.datasource.database.data["parameters"]["database"]}.{qc.datasource.name}""",
        "schema": [{"name": c.column_name, "field_type": str(c.type), "nullable": True} for c in qc.datasource.columns]
    }
    from superset.charts.dao import ChartDAO
    chart_id = qc.form_data["slice_id"]
    chart_slice = ChartDAO.find_by_id(chart_id)
    chart_name = str(chart_slice) # returns the name of the slice
    from superset.dashboards.dao import DashboardDAO
    dashboard_id = qc.form_data["dashboardId"]
    dashboard = DashboardDAO.find_by_id(dashboard_id)
    dashboard_dashboard_title = dashboard.dashboard_title
    
    if len(chart_data_result_payload["queries"]) > 1:
      logging.warning(f""""A chart (id: {chart_id}, name: {chart_name}) command payload returned more that one queries ({len(chart_data_result_payload["queries"])}), currently only one is supported""")
    first_query = chart_data_result_payload["queries"][0] # TODO when is there several?
    out_ds = {
        "name": f"""Superset:{dashboard_dashboard_title}:{chart_name}""",
        "format": "Superset:Chart",
        "location": f"""/superset/explore/?form_data={{"slice_id":{chart_id}}}""",
        "schema": [{"name": n, "field_type": str(t), "nullable": True} for n, t in zip(first_query["colnames"], first_query["coltypes"])]
    }
    out_ds["metrics"] = KensuPandasSupport().extract_stats(pd.DataFrame.from_records(first_query['data']))

    in_triplet = to_kensu(in_ds)
    out_triplet = to_kensu(out_ds)
    # TODO? => currently reporting full mapping
    lineage = ProcessLineage(name="Superset chart", operation_logic="Ignore", 
                            pk=ProcessLineagePK(process_ref = agent.process.to_ref(), 
                                                data_flow = [
                                                    SchemaLineageDependencyDef( from_schema_ref=in_triplet[1].to_ref(), to_schema_ref=out_triplet[1].to_ref(),
                                                                                column_data_dependencies={
                                                                                    o.name: [i.name for i in in_triplet[1].pk.fields] for o in out_triplet[1].pk.fields
                                                                                })]))
    lineage._report()
    lineage_run = LineageRun(pk=LineageRunPK(process_run_ref=agent.process_run.to_ref(), lineage_ref=lineage.to_ref(), 
                            timestamp=round(datetime.datetime.now().timestamp()*1000)))
    lineage_run._report()
    if in_triplet[2] is not None:
        in_triplet[2].pk.lineage_run_ref=lineage_run.to_ref()
        in_triplet[2]._report()
    if out_triplet[2] is not None:
        out_triplet[2].pk.lineage_run_ref=lineage_run.to_ref()
        out_triplet[2]._report()
    