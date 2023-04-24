import pandas as pd
import os
import json


def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = json.JSONEncoder().default
json.JSONEncoder.default = _default
pd.options.mode.chained_assignment = None


class ApplicationRepository:
    location: str

    def __init__(self, location: str) -> None:
        self.location = location

    def to_json(self):
        return {"location": self.location}

    @staticmethod
    def fetch_git_location():
        import git
        code_repo = git.Repo(os.getcwd(), search_parent_directories=True).remote().url
        return code_repo


class Application:
    name: str

    def __init__(self, name: str, repository: ApplicationRepository) -> None:
        self.name = name
        self.repository = repository

    def to_json(self):
        return {"name": self.name, "repository": self.repository.to_json()}

    @staticmethod
    def fetch_file_name():
        import os
        application_name = os.path.basename(os.path.realpath(__file__))
        return application_name


app_repo = ApplicationRepository(ApplicationRepository.fetch_git_location())
app = Application(Application.fetch_file_name(), app_repo)

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
