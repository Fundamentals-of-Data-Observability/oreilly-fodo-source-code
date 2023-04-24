import pandas as pd
import os
import json
import uuid


def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = json.JSONEncoder().default
json.JSONEncoder.default = _default
pd.options.mode.chained_assignment = None


class Application:
    id: str = str(uuid.uuid4())
    name: str

    def __init__(self, name: str) -> None:
        self.name = name

    def to_json(self):
        return {"id": self.id, "name": self.name}

    @staticmethod
    def fetch_file_name():
        import os
        application_name = os.path.basename(os.path.realpath(__file__))
        return application_name


class ApplicationRepository:
    id: str = str(uuid.uuid4())
    location: str
    application: Application

    def __init__(self, location: str, application: Application) -> None:
        self.location = location
        self.application = application

    def to_json(self):
        return {"id": self.id, "location": self.location, "application": self.application.id}

    @staticmethod
    def fetch_git_location():
        import git
        code_repo = git.Repo(os.getcwd(), search_parent_directories=True).remote().url
        return code_repo


app = Application(Application.fetch_file_name())
app_repo = ApplicationRepository(ApplicationRepository.fetch_git_location(), app)

AppTech = pd.read_csv(
    "data/AppTech.csv",
    parse_dates=["Date"],
    dtype={"Symbol": "category"},
)
Buzzfeed = pd.read_csv(
    "data/Buzzfeed.csv",
    parse_dates=["Date"],
    dtype={"Symbol": "category"},
)

monthly_assets = pd.concat([AppTech, Buzzfeed]) \
    .astype({"Symbol": "category"})
monthly_assets.to_csv(
    "data/monthly_assets.csv", index=False
)

all_assets = pd.read_csv("data/monthly_assets.csv", parse_dates=['Date'])

apptech = all_assets[all_assets['Symbol'] == 'APCX']
buzzfeed = all_assets[all_assets['Symbol'] == 'BZFD']

buzzfeed['Intraday_Delta'] = buzzfeed['Adj Close'] - buzzfeed['Open']
apptech['Intraday_Delta'] = apptech['Adj Close'] - apptech['Open']

kept_values = ['Open', 'Adj Close', 'Intraday_Delta']

buzzfeed[kept_values].to_csv("data/report_buzzfeed.csv", index=False)
apptech[kept_values].to_csv("data/report_appTech.csv", index=False)

print(json.dumps(app))
print(json.dumps(app_repo))
