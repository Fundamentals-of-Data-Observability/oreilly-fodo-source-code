import pandas as pd
import os
import getpass
import git
import datetime

pd.options.mode.chained_assignment = None
app_user = getpass.getuser()
repo = git.Repo(os.getcwd(), search_parent_directories=True)
code_repo = repo.remote().url
commit = repo.head.commit
code_version = commit.hexsha
code_author = commit.author.name
application_name = os.path.basename(os.path.realpath(__file__))
application_start_time = datetime.datetime.now().isoformat()

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

application_observations = {
    "name": application_name,
    "code": {
        "repo": code_repo,
        "version": code_version,
        "author": code_author
    },
    "execution": {
        "start": application_start_time,
        "user": app_user
    }
}

print(application_observations)
