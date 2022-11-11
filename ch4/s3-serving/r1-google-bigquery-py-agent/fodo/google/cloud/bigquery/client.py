from google.cloud.bigquery import client, Dataset
import logging

logger = logging.getLogger(__name__)

from .job.query import QueryJob
import google.cloud.bigquery as bq


# Injection by delegation 
class Client(client.Client):

    # Make Client.query data observable: it execute a queries, which can also be an "insert into" or a "merge"
    def query(
        self,
        query,
        job_config=None,
        job_id=None,
        job_id_prefix=None,
        location=None,
        project=None,
        retry=DEFAULT_RETRY,
        timeout=None
    ):
        # Return the original client (self is the monkey patched)
        gclient = super(Client, self)

        # call the query method on the original client
        job = gclient.query(query, job_config, job_id, job_id_prefix, location, project, retry, timeout)

        # 1. Instantiate the data observability agent (if not already started)

        for _qu in query.split(";"):
            # 2. Extract DAG from the query. From: sqlparse.parse(query) (not fully compliant with BQ)

            # For queries "insert into" or "merge", the execution of the action is performed now
            # Hence, `result` is not called on the returning job instance to retrieve the data (as per `select`).
            # So we are taking care of generating the data observations here, instead of delegating to `result` 
            # (`result` is in fact a delayed action)

            # If `_qu` is an "insert" or "merge"
                # For each table in the DAG: 
                # - 3. get metadata bq_client.get_table(table_id)
                # - 4. compute metrics: generate SQLs to fetch metrics such count, nulls, min, max, ...

                # 5. From the DAG: create lineage between output and inputs
            pass

        # HINT: as this job's result is not yet available and can be used in this script after retrieval (e.g., w/ using pandas)
        # 6. We patch the QueryJob instance's functions to continue the tracking of the data usages
        return QueryJob.patch(job)


    # Make Client.load_table_from_uri observable (it fetches input sources [csv, ...] and loads them in a destination table )
    def load_table_from_uri(
        self,
        source_uris,
        destination,
        job_id = None,
        job_id_prefix = None,
        location = None,
        project = None,
        job_config = None,
        retry = DEFAULT_RETRY,
        timeout = None
    ):
        # Return the original client (self is the monkey patched)
        gclient = super(Client, self)

        # call the load_table_from_uri method on the original client
        job = gclient.load_table_from_uri(source_uris, destination, job_id, job_id_prefix, location, project, job_config, retry, timeout)

        # Performing the load
        block = job.result()

        # 1. Instantiate the data observability agent (if not already started)

        # 2. Retrieve from BigQuery metadata about the `destination` table

        for ds_uri_location in source_uris:
            # For each uri provided that should be copied to the `destination`
            # 3. Create a dedicated Data Source entity, and attach (at least) the schema of the `destination` as we load as-is

            # 4. Create lineage between this uri based Data Source and Schema with the one created for the `destination`

            pass

        # The data is not provided in the returned job, so no need to patch it.
        # (The data in the destination table has to be queried again to be used--using the `query` method above)
        return job
