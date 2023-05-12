# Credits of original script:
# KRISHNENDU S. KAR on Kaggle
# https://www.kaggle.com/code/ksekhark/simple-linear-regression-using-sklearn/notebook

import sys
from kensu.client import *
from kensu.utils.kensu_provider import KensuProvider
import urllib3
urllib3.disable_warnings()
agent = KensuProvider().initKensu(process_name=sys.argv[0],init_context=True,allow_reinit=True,do_report=True,project_name="sklearn",get_explicit_code_version_fn=lambda: "1.0.0")

import kensu.pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

# Load model
from kensu.pickle import load
regressor = load(open('../data/model.pickle', 'rb'))

## Predict on new input
data = pd.read_csv('../data/data.csv')
data["prediction"] = regressor.predict(data.iloc[:,0:1].values)

# Serve result
data.to_csv("../data/predictions.csv")