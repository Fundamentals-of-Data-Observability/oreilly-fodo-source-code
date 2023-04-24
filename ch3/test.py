import pandas as pd
pd.options.mode.chained_assignment = None

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
