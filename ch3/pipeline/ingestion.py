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
