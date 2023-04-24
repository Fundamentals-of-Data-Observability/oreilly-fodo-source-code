# Pygmented with https://pygments.org/demo/
# Formatter: Default
# Style: Default


# ======= Code 3-0-1

import pandas as pd

AppTech = pd.read_csv(
    "../datasources/%s/%s/Apple.csv" % (year, month),
    parse_dates=["Date"],
    dtype={"Symbol": "category"},
    )
Buzzfeed = pd.read_csv(
    "../datasources/%s/%s/Buzzfeed.csv" % (year, month),
    parse_dates=["Date"],
    dtype={"Symbol": "category"},
    )

monthly_assets = pd.concat([ AppTech, Buzzfeed ]) \
    .astype({"Symbol": "category"})
monthly_assets.to_csv(
    "../datasources/%s/%s/monthly_assets.csv" % (year, month), index=False
)

# ======= Code 3-0-2

import pandas as pd

all_assets = pd.read_csv("../datasources/%s/%s/monthly_assets.csv"%(year,month),parse_dates=['Date'])

apptech = all_assets[all_assets['Symbol'] == 'APCX']
Buzzfeed = all_assets[all_assets['Symbol'] == 'ENFA']

Buzzfeed['Intraday_Delta'] = Buzzfeed['Adj Close'] - Buzzfeed['Open']
apptech['Intraday_Delta'] = apptech['Adj Close'] - apptech['Open']

kept_values = ['Open','Adj Close','Intraday_Delta']

Buzzfeed[kept_values].to_csv("../datasources/%s/%s/report_buzzfeed.csv"%(year,month),index=False)
apptech[kept_values].to_csv("../datasources/%s/%s/report_AppTech.csv"%(year,month),index=False)

# ======= Code 3-1

# retrieve information about current user
import getpass
app_user = getpass.getuser()

# retrieve information about application code from git
import git
# - the remote git repo
code_repo = git.Repo(cur_dir, search_parent_directories=True).remote()
# fetching the latest commit
commit = git_repo.head.commit
# - the latest commit hash is used as version (note: we could use something from setting.py alternatively)
code_version = commit.hexsha
# - the latest commit author is considered as the user connected to this version
code_author = commit.author.name

# retrieve application name as the file name
application_name = os.path.basename(os.path.realpath(__file__))

# retrieve information about server on which this application runs on - example, using IP
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(('10.254.254.254', 1))
server_ip = s.getsockname()[0]
s.close()

# retrieve starting time of the application
import datetime
application_start_time = datetime.datetime.fromtimestamp(timestamp/1000).isoformat()

# ======= Code 3-2

application_observations = {
    "name": application_name,
    "code": {
        "repo": code_repo,
        "version": code_version,
        "author": code_author
    },
    "execution": {
        "server": server_ip
        "start": application_start_time,
        "user": app_user
    }
}
import json
logging.info(json.dumps(application_observations))

# ======= Code 3-3

# Class encoding the Application Repostiroy entity
class ApplicationRepository:
    location: str
    application: Application #TODO: not clear
    def __init__(self, id: str, location: str, application: Application) -> None:
        self.location = location
        self.application = application
    def to_json(self):
        return {"location": self.location}

# Class encoding the Application entity that refers to its repo
class Application:
    name: str
    def __init__(self, name: str, repository: ApplicationRepository) -> None:
        self.name = name
        self.repository = repository
    def to_json(self):
        return {"name": self.name, "repository": self.repository.to_json()}

# instanciation and reporting (log)
app_repo = ApplicationRepository(code_repo)
app = Application(application_name, app_repo)
logging.info(json.dumps(app))

# ======= Code 3-4

# Class encoding the Application Repository entity
class ApplicationRepository:
    location: str
    application: Application
    def __init__(self, id: str, location: str, application: Application) -> None:
        self.location = location
        self.application = application
    def to_json(self):
        return {"location": self.location}

    # Add static method to fetch the git location
    @staticmethod
    def fetch_git_location():
        import git
        code_repo = git.Repo(cur_dir, search_parent_directories=True).remote()
        return code_repo

# Class encoding the Application entity that refers to its current repo
class Application:
    name: str
    def __init__(self, name: str, repository: ApplicationRepository) -> None:
        self.name = name
        self.repository = repository
    def to_json(self):
        return {"name": self.name, "repository": self.repository.to_json()}

    # Add static method to fetch the name as the file name of the current script
    @staticmethod
    def fetch_file_name():
        import os
        application_name = os.path.basename(os.path.realpath(__file__))
        return application_name

# instanciation and reporting (log)
app_repo = ApplicationRepository(ApplicationRepository.fetch_git_location())
app = Application(Application.fetch_file_name(), app_repo)
logging.info(json.dumps(app))

# ======= Code 3-5

class Application:
    name: str
    def __init__(self, name: str, version: ApplicationVersion, repo: ApplicationRepository, execution: ApplicationExecution, server: Server, author: User) -> None:
        pass

# ======= Code 3-6 #todo: why changing the order?

import uuid
class Application:
    id: str = str(uuid.uuid4())
    name: str
    def __init__(self, name: str) -> None:
        self.name = name
    def to_json(self):
        return {"id": self.id, "name": self.name}
    # Add static method to fetch the name as the file name of the current script
    @staticmethod
    def fetch_file_name():
        import os
        application_name = os.path.basename(os.path.realpath(__file__))
        return application_name

class ApplicationRepository :
    id: str = str(uuid.uuid4())
    location: str
    application: Application
    def __init__(self, id: str, location: str, application: Application) -> None:
        self.location = location
        self.application = application
    def to_json(self):
        return {"id": self.id, "location": self.location, "application": self.application.id}
    # Add static method to fetch the git repo
    @staticmethod
    def fetch_git_location():
        import git
        code_repo = git.Repo(cur_dir, search_parent_directories=True).remote()
        return code_repo

# instanciation and reporting (log)
app = Application(Application.fetch_file_name())
logging.info(json.dumps(app))

app_repo = ApplicationRepository(ApplicationRepository.fetch_git_location(), app)
logging.info(json.dumps(app_repo))

# ======= Code 3-7

import hashlib
class Application:
    # primary key
    name: str
    id: str
    def __init__(self, name: str) -> None:
        self.name = name
        self.id = hashlib.md5(self.name.encode("utf-8")).hexdigest()
    def to_json(self):
        return {"id": self.id, "name": self.name}
    # Add static method to fetch the name as the file name of the current script
    @staticmethod
    def fetch_file_name():
        import os
        application_name = os.path.basename(os.path.realpath(__file__))
        return application_name
class ApplicationRepository:
    # primary key
    location: str
    # primary key (the repo is then unique for a given application in our case)
    application: Application
    id: str
    def __init__(self, id: str, location: str, application: Application) -> None:
        self.location = location
        self.application = application
        self.id = hashlib.md5(",".join([self.location, self.application.id]).encode("utf-8")).hexdigest()
    def to_json(self):
        return {"id": self.id, "location": self.location, "application": self.application.id}
    # Add static method to fetch the git repo
    @staticmethod
    def fetch_git_location():
        import git
        code_repo = git.Repo(cur_dir, search_parent_directories=True).remote()
        return code_repo

# instanciation and reporting (log)
app = Application(Application.fetch_file_name())
logging.info(json.dumps(app))

app_repo = ApplicationRepository(ApplicationRepository.fetch_git_location(), app)
logging.info(json.dumps(app_repo))

# ======= Code 3-7

class DataSource:
    # primary key
    location: str
    # primary key
    server:Server
    format: str
    id: str
    def __init__(self, location: str, server:Server, format: str = None) -> None:
        self.location = location
        self.server = server
        self.id = hashlib.md5(",".join([self.location, self.server.id]).encode("utf-8")).hexdigest()
    def to_json(self):
        return {"id": self.id, "location": self.location, "server": self.server.id}
    # Add static method to extract the server information from a file_path
    @staticmethod
    def extract_server(file_path):
        # overly simple, quick and dirty heuristic
        if file_path.startswith("file:/") or file_path.startswith(".") or file_path.startswith("/"):
            return Server(Server.get_local_ip())
        # can handle ftp, hdfs, https, ...
        return None

# ======= Code 3-8

class Schema:
    # primary key
    # fields is a list of (`name`, `type`) - each entry is a field / a column information
    fields: list[tuple[str, str]]
    # primary key => so fields are linked to this data source only in our model
    data_source:DataSource
    id: str
    def __init__(self, fields: str, data_source: DataSource) -> None:
        self.fields = fields
        self.data_source = data_source
        linearized_fields = ",".join(list(map(lambda x: x[0]+"-"+x[1], sorted(self.fields))))
        self.id = hashlib.md5(",".join([linearized_fields, self.data_source.id]).encode("utf-8")).hexdigest()
    def to_json(self):
        from functools import reduce
        jfields = reduce(lambda x,y:dict(**x,**y), map(lambda f: {f[0]: f[1]}, self.fields))
        return {"id": self.id, "fields": jfields, "data_source": self.data_source.id}
    # Add static method to extract the fields information from a pandas DataFrame
    @staticmethod
    def extract_fields_from_dataframe(df:pd.DataFrame, hints: dict):
        fs = list(zip(df.columns.values.tolist(), map(lambda x: str(x), df.dtypes.values.tolist())))
        return [(f[0], hints.get(f[0], f[1])) for f in fs]

# ======= Code 3-9

class DataMetrics:
    # primary key => the metrics is attached to the schema that was just read, as the data source could change
    schema:Schema
    # metrics is a list of names and values - which in this case will be only numerical.
    # We'll first represent the numerical values returned by `describe`
    metrics: list[tuple[str, float]]

    # ⚠️⚠️⚠️⚠️ SOMETHING IS MISSING => see next section when we introduce the metrics context: Lineage ⚠️⚠️⚠️⚠️

    id: str
    def __init__(self, metrics: list[tuple[str, float]], schema: Schema) -> None:
        self.metrics = metrics
        self.schema = schema
        self.id = hashlib.md5(",".join([self.schema.id]).encode("utf-8")).hexdigest()
    def to_json(self):
        return {"id": self.id, "metrics": jfields, "schema": self.schema.id}
    # Add static method to extract the metrics information from DataFrame's describe
    @staticmethod
    def extract_metrics_from_dataframe(df:pd.DataFrame):
        d = df.describe(include='all')
        import math
        import numbers
        metrics = {}
        for field in d.columns[1:]:
            msd = dict(filter(lambda x: isinstance(x[1], numbers.Number) and math.isnan(x[1]) is False,
                              map(lambda x: (field+"."+x[0], x[1]), d[field].to_dict().items())))
            metrics.update(msd)
        return metrics

# ======= Code 3-10

apple_path = "../datasources/%s/%s/Apple.csv" % (year, month)
apple_hints = {"Date": "date", "Symbol": "category"}
Apple = pd.read_csv(apple_path, parse_dates=list(dict(filter(lambda x: x[1] == "date", apple_hints.items())).keys()),
                    dtype=dict(filter(lambda x: x[1] == "category", apple_hints.items())))
Apple_data_source = DataSource(apple_path, DataSource.extract_server(apple_path)))
logging.info(json.dumps(Apple_data_source))
Apple_schema = Schema(Schema.extract_fields_from_dataframe(Apple, apple_hints), Apple_data_source)
logging.info(json.dumps(Apple_schema))
Apple_data_metrics = DataMetrics(DataMetrics.extract_metrics_from_dataframe(Apple), Apple_schema)
logging.info(json.dumps(Apple_data_metrics))

buzzfeed_path = "../datasources/%s/%s/Buzzfeed.csv" % (year, month)
buzzfeed_hints = {"Date": "date", "Symbol": "category"}
Buzzfeed = pd.read_csv(buzzfeed_path, parse_dates=list(dict(filter(lambda x: x[1] == "date", buzzfeed_hints.items())).keys()), ,
dtype=dict(filter(lambda x: x[1] == "category", buzzfeed_hints.items())))
Buzzfeed_data_source = DataSource(buzzfeed_path, DataSource.extract_server(buzzfeed_path)))
logging.info(json.dumps(Buzzfeed_data_source))
Buzzfeed_schema = Schema(Schema.extract_fields_from_dataframe(Buzzfeed, buzzfeed_hints), Buzzfeed_data_source)
logging.info(json.dumps(Buzzfeed_schema))
Buzzfeed_data_metrics = DataMetrics(DataMetrics.extract_metrics_from_dataframe(Buzzfeed), Buzzfeed_schema)
logging.info(json.dumps(Buzzfeed_data_metrics))


# [...]

monthly_assets_path = "../datasources/%s/%s/monthly_assets.csv" % (year, month)
monthly_assets.to_csv(
    monthly_assets_path, index=False
)
monthly_assets_data_source = DataSource(monthly_assets_path, DataSource.extract_server(monthly_assets_path)))
logging.info(json.dumps(monthly_assets_data_source))
monthly_assets_schema = Schema(Schema.extract_fields_from_dataframe(monthly_assets), monthly_assets_data_source)
logging.info(json.dumps(monthly_assets_schema))
monthly_assets_data_metrics = DataMetrics(DataMetrics.extract_metrics_from_dataframe(monthly_assets), monthly_assets_schema)
logging.info(json.dumps(monthly_assets_data_metrics))


# ======= Code 3-11 #todo: we can simplyfy everything by removing "hint" complexity

def observations_for_df(params: dict, df: DataFrame) -> None
    hints = dict(**{d : "date" for d in params["kwargs"]["parse_dates"]}, **params["kwargs"]["dtype"])
    ds = DataSource(path, DataSource.extract_server(params["args"][0])))
    logging.info(json.dumps(ds))
    sc = Schema(Schema.extract_fields_from_dataframe(df, hints), ds)
    logging.info(json.dumps(sc))
    ms = DataMetrics(DataMetrics.extract_metrics_from_dataframe(df), sc)
    logging.info(json.dumps(ms))

    apple_read_params = {
    "args": ["../datasources/%s/%s/Apple.csv" % (year, month)],
    "kwargs": {"parse_dates": ["Date"], "dtype": {"Symbol": "category"}}
}
Apple = pd.read_csv(*apple_read_params["args"], **apple_read_params["kwargs"])
observations_for_df(apple_read_params, Apple)

buzzfeed_read_params = {
"args": ["../datasources/%s/%s/Buzzfeed.csv" % (year, month)],
"kwargs": {"parse_dates": ["Date"], "dtype": {"Symbol": "category"}}
}
Buzzfeed = pd.read_csv(*buzzfeed_read_params["args"], **buzzfeed_read_params["kwargs"])
observations_for_df(buzzfeed_read_params, Buzzfeed)

# [...]

monthly_assets_write_params = {
"args": ["../datasources/%s/%s/monthly_assets.csv" % (year, month)],
"kwargs": {"index": False}
}
monthly_assets.to_csv(*monthly_assets_write_params["args"], **monthly_assets_write_params["kwargs"])
observations_for_df(monthly_assets_write_params, monthly_assets)


# ======= Code 3-12

class OutputDataLineage:
    # primary key - the schema of the data source output by this lineage
    schema: Schema
    # primary key - each schema of data sources used as input are mapped at field level as a dict (OutputFieldName, list[InputFieldName])
    input_schemas_mapping: list[tuple(Schema, dict)]
    def __init__(self, schema: str, input_schemas_mapping:list[tuple(Schema, dict)]) -> None:
        self.schema = schema
        self.input_schemas_mapping = input_schemas_mapping
        self.id = hashlib.md5(",".join([self.schema.id, linearize(input_schemas_mapping))]).encode("utf-8")).hexdigest()
    def to_json(self):
        return {"id": self.id, "schema": self.schema, "input_schemas_mapping": self.input_schemas_mapping}
    # Generate a mapping using a simple heuristic that use field names only to create dependencies
    @staticmethod
    def generate_direct_mapping(output_schema: Schema, input_schemas: list[Schema]):
        input_schemas_mapping = []
        # get output field names only
        output_schema_field_names = [f[0] for f in output_schema.fields]
        # let's analyze each input schema and see which ones have a direct link with the output
        for schema in input_schemas:
            mapping = {}
            # loop over the input fields
            for field in input_schemas.fields:
                # if the input field is in the ouput fields
                if field[0] in output_schema_field_names:
                    # then we map them
                    mapping[field[0]] = [field[0]]
            # if at least one map has been added
            if len(mapping):
                # then we create a connection with the output schema
                input_schemas_mapping.append((schema, mapping))
        return input_schemas_mapping
    def linearize(input: list[tuple(Schema, dict)]):
        # left blank for the purpose of this example.
        pass

class DataLineageExecution:
    # primary key
    lineage: OutputDataLineage
    # primary key - links the current execution of the transformation with the execution of the application that runs it
    application_execution: ApplicationExecution
    # primary key - we make the choice to add the start time in the PK as we may have several runs of the same lineage
    start_time: datetime
    # NB: we could have a "end_time" as well which may not be in the PK perhaps.
    def __init__(self, lineage: OutputDataLineage, application_execution:ApplicationExecution) -> None:
        self.lineage = lineage
        self.application_execution = application_execution
        self.start_time = datetime.datetime.now()
        self.id = hashlib.md5(",".join([self.lineage.id, self.application_execution.id, self.start_time)]).encode("utf-8")).hexdigest()
    def to_json(self):
        return {"id": self.id, "lineage": self.lineage, "application_execution": self.application_execution, "start_time": self.start_time}

# ======= Code 3-13

class DataMetrics:
    # primary key => the metrics is attached to the schema that was just read, as the data source could change
    schema:Schema
    # primary key => the lineage execution to ensure the metrics is "orphan"
    lineage_execution: DataLineageExecution

    metrics: list[tuple[str, float]]

    id: str
    def __init__(self, metrics: list[tuple[str, float]], schema: Schema, lineage_execution: DataLineageExecution) -> None:
        self.metrics = metrics
        self.schema = schema
        self.lineage_execution = lineage_execution
        self.id = hashlib.md5(",".join([self.schema.id, self.lineage_execution.id]).encode("utf-8")).hexdigest()
    def to_json(self):
        return {"id": self.id, "metrics": jfields, "schema": self.schema.id, "lineage_execution": self.lineage_execution.id}
    # Add static method to extract the metrics information from DataFrame's describe
    @staticmethod
    def extract_metrics_from_dataframe(df:pd.DataFrame):
        d = df.describe(include='all')
        import math
        import numbers
        metrics = {}
        for field in d.columns[1:]:
            msd = dict(filter(lambda x: isinstance(x[1], numbers.Number) and math.isnan(x[1]) is False,
                              map(lambda x: (field+"."+x[0], x[1]), d[field].to_dict().items())))
            metrics.update(msd)
        return metrics

# ======= Code 3-14

# Observations high-level helpers -> move to a framework
def andLog(val):
    logging.info(json.dumps(val))
    return val
# generate observations about the application and return what is reused: the ApplicationExecution
def observations_for_application():
    application = andLog(Application(Application.fetch_file_name()))
    application_repo = andLog(ApplicationRepository(ApplicationRepository.fetch_git_location()), application)
    application_version = andLog(ApplicationVersion(ApplicationVersion.fetch_git_version(), User(ApplicationVersion.fetch_git_author()), application_repo))
    application_execution = andLog(ApplicationExecution(application_version, User(User.fetch.current_user()), Server(Server.fetch_ip())))
    return application_execution

# Now the helper generate observations for DataSource and Schema,
# but only prepares the generation of the DataMetrics for when the lineage execution will be available
# Hence why the function now returns the entities
def observations_for_df(params: dict, df: DataFrame):
    hints = dict(**{d : "date" for d in params["kwargs"]["parse_dates"]}, **params["kwargs"]["dtype"])
    ds = andLog(DataSource(path, DataSource.extract_server(params["args"][0]))))
    sc = andLog(Schema(Schema.extract_fields_from_dataframe(df, hints), ds))
    logging.info(json.dumps(sc))
    def generate_metrics(lineage_execution: DataLineageExecution):
        return andLog(DataMetrics(DataMetrics.extract_metrics_from_dataframe(df), sc, lineage_execution))
    return (ds, sc, generate_metrics)

# Script starts here

application_execution = observations_for_application()

apple_read_params = {
    "args": ["../datasources/%s/%s/Apple.csv" % (year, month)],
    "kwargs": {"parse_dates": ["Date"], "dtype": {"Symbol": "category"}}
}
Apple = pd.read_csv(*apple_read_params["args"], **apple_read_params["kwargs"])

buzzfeed_read_params = {
    "args": ["../datasources/%s/%s/Buzzfeed.csv" % (year, month)],
    "kwargs": {"parse_dates": ["Date"], "dtype": {"Symbol": "category"}}
}
Buzzfeed = pd.read_csv(*buzzfeed_read_params["args"], **buzzfeed_read_params["kwargs"])

# [...] other inputs

monthly_assets_write_params = {
    "args": ["../datasources/%s/%s/monthly_assets.csv" % (year, month)],
    "kwargs": {"index": False}
}
monthly_assets.to_csv(*monthly_assets_write_params["args"], **monthly_assets_write_params["kwargs"])

# finish with all observations generated and sent
observations = {"inputs": [], "output": None}
observations["inputs"].append(observations_for_df(apple_read_params, Apple))
observations["inputs"].append(observations_for_df(buzzfeed_read_params, Buzzfeed))
# [...] for other inputs
observations["output"] = observations_for_df(monthly_assets_write_params, monthly_assets)
# generate lineage based on inputs for the output
lineage = OutputDataLineage(observations["output"][1], OutputDataLineage.generate_direct_mapping(observations["output"][1], [i[1] for i in observations["inputs"]]))
lineage_execution = DataLineageExecution(lineage, application_execution)
# updating metrics
for o in observations["inputs"]: o[2](lineage_execution)
observations["output"][2](lineage_execution)

# ======= Code 3-15

import sys

month = sys.argv[1]
year = sys.argv[2]

application_execution = observations_for_application()

import pandas as pd

all_assets_read_params = {
    "args": ["../datasources/%s/%s/monthly_assets.csv"%(year,month)],
    "kwargs": {"parse_dates": ["Date"]}
}
all_assets = pd.read_csv(*all_assets_read_params["args"], **all_assets_read_params["kwargs"])

apptech = all_assets[all_assets['Symbol'] == 'APCX']
Buzzfeed = all_assets[all_assets['Symbol'] == 'ENFA']

Buzzfeed['Intraday_Delta'] = Buzzfeed['Adj Close'] - Buzzfeed['Open']
apptech['Intraday_Delta'] = apptech['Adj Close'] - apptech['Open']

kept_values = ['Open','Adj Close','Intraday_Delta']

Buzzfeed_write_params = {
    "args": ["../datasources/%s/%s/report_buzzfeed.csv"%(year,month)],
    "kwargs": {"index": False}
}
Buzzfeed[kept_values].to_csv(*Buzzfeed_write_params["args"], **Buzzfeed_write_params["kwargs"])
apptech_write_params = {
    "args": ["../datasources/%s/%s/report_AppTech.csv"%(year,month)],
    "kwargs": {"index": False}
}
apptech[kept_values].to_csv(*apptech_write_params["args"], **apptech_write_params["kwargs"])

# finish with all observations generated and sent
obs_all = observations_for_df(all_assets_read_params, all_assets)

obs_buzzfeed = observations_for_df(Buzzfeed_write_params, Buzzfeed)
lineage_buzzfeed = OutputDataLineage(obs_buzzfeed[1], OutputDataLineage.generate_direct_mapping(obs_buzzfeed[1], [obs_all[1]]))
lineage_buzzfeed_execution = DataLineageExecution(lineage_buzzfeed, application_execution)
obs_all[2](lineage_buzzfeed_execution)
obs_buzzfeed[2](lineage_buzzfeed_execution)

obs_apptech = observations_for_df(apptech_write_params, apptech)
lineage_apptech = OutputDataLineage(obs_apptech[1], OutputDataLineage.generate_direct_mapping(obs_apptech[1], [obs_all[1]]))
lineage_apptech_execution = DataLineageExecution(lineage_apptech, application_execution)
# ⚠️ this is the second time we generate a metrics for all_assets.
obs_all[2](lineage_apptech_execution)
obs_apptech[2](lineage_apptech_execution)

# ======= Code 3-16

# Generate a mapping using a simple heuristic that use field names only to create dependencies
# Now each Schema is accompanied with hardcoded extra mapping in case direct is not enough
@staticmethod
def generate_direct_mapping(output_schema: Schema, input_schemas: list[tuple(Schema, dict)]):
    input_schemas_mapping = []
    # get output field names only
    output_schema_field_names = [f[0] for f in output_schema.fields]
    # let's analyze each input schema and see which ones have a direct link with the output
    for (schema, extra_mapping) in input_schemas:
        mapping = {}
        # loop over the input fields
        for field in input_schemas.fields:
            # if the input field is in the ouput fields
            if field[0] in output_schema_field_names:
                # then we map them
                mapping[field[0]] = [field[0]]
        # updating the mapping with the provided extra mapping if any
        mapping.update(extra_mapping)
        # if at least one map has been added
        if len(mapping):
            # then we create a connection with the output schema
            input_schemas_mapping.append((schema, mapping))
    return input_schemas_mapping

# ======= Code 3-16

# finish with all observations generated and sent
obs_all = observations_for_df(all_assets_read_params, all_assets)

# manually mapping the "Intraday_Delta" column
intraday_delta_mapping = {"Intraday_Delta": ['Adj Close'], 'Open']}

obs_buzzfeed = observations_for_df(Buzzfeed_write_params, Buzzfeed)
lineage_buzzfeed = OutputDataLineage(obs_buzzfeed[1], OutputDataLineage.generate_direct_mapping(obs_buzzfeed[1], [(obs_all[1], intraday_delta_mapping)]))
lineage_buzzfeed_execution = DataLineageExecution(lineage_buzzfeed, application_execution)
obs_all[2](lineage_buzzfeed_execution)
obs_buzzfeed[2](lineage_buzzfeed_execution)

obs_apptech = observations_for_df(apptech_write_params, apptech)
lineage_apptech = OutputDataLineage(obs_apptech[1], OutputDataLineage.generate_direct_mapping(obs_apptech[1], [(obs_all[1], intraday_delta_mapping)]))
lineage_apptech_execution = DataLineageExecution(lineage_apptech, application_execution)
# ⚠️ this is the second time we generate a metrics for all_assets.
obs_all[2](lineage_apptech_execution)
obs_apptech[2](lineage_apptech_execution)

###


import sys

month = sys.argv[1]
year = sys.argv[2]

import pandas as pd

Apple = pd.read_csv(
"../datasources/%s/%s/Apple.csv" % (year, month),
parse_dates=["Date"],
dtype={"Symbol": "category"},
)
Buzzfeed = pd.read_csv(
"../datasources/%s/%s/Buzzfeed.csv" % (year, month),
parse_dates=["Date"],
dtype={"Symbol": "category"},
)
EURUSD = pd.read_csv(
"../datasources/%s/%s/EURUSD.csv" % (year, month),
parse_dates=["Date"],
dtype={"Symbol": "category"},
)
Microsoft = pd.read_csv(
"../datasources/%s/%s/Microsoft.csv" % (year, month),
parse_dates=["Date"],
dtype={"Symbol": "category"},
)
iMetal = pd.read_csv(
"../datasources/%s/%s/iMetal.csv" % (year, month),
parse_dates=["Date"],
dtype={"Symbol": "category"},
)
AppTech = pd.read_csv(
"../datasources/%s/%s/AppTech.csv" % (year, month),
parse_dates=["Date"],
dtype={"Symbol": "category"},
)

monthly_assets = pd.concat([ Apple, Buzzfeed, EURUSD, Microsoft, iMetal, AppTech ]) \
    .astype({"Symbol": "category"})
monthly_assets.to_csv(
"../datasources/%s/%s/monthly_assets.csv" % (year, month), index=False
)

###

import sys

month = sys.argv[1]
year = sys.argv[2]

import pandas as pd

all_assets = pd.read_csv("../datasources/%s/%s/monthly_assets.csv"%(year,month),parse_dates=['Date'])

apptech = all_assets[all_assets['Symbol'] == 'APCX']
Buzzfeed = all_assets[all_assets['Symbol'] == 'ENFA']

Buzzfeed['Intraday_Delta'] = Buzzfeed['Adj Close'] - Buzzfeed['Open']
apptech['Intraday_Delta'] = apptech['Adj Close'] - apptech['Open']

kept_values = ['Open','Adj Close','Intraday_Delta']

Buzzfeed[kept_values].to_csv("../datasources/%s/%s/report_buzzfeed.csv"%(year,month),index=False)
apptech[kept_values].to_csv("../datasources/%s/%s/report_AppTech.csv"%(year,month),index=False)

###
