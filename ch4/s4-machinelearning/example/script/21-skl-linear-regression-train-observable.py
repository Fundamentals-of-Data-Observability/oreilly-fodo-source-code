# Credits of original script:
# KRISHNENDU S. KAR on Kaggle
# https://www.kaggle.com/code/ksekhark/simple-linear-regression-using-sklearn/notebook

import sys
from kensu.client import *
from kensu.utils.kensu_provider import KensuProvider
import urllib3
urllib3.disable_warnings()
agent = KensuProvider().initKensu(process_name=sys.argv[0],init_context=True,allow_reinit=True,do_report=True,project_name="sklearn",get_explicit_code_version_fn=lambda: "1.0.0")

import kensu.numpy as np # linear algebra
import kensu.pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

data = pd.read_csv('../data/train.csv')

# Removing any data point above x = 100
# There is only one record
data = data[data.x <= 100]

# Separating dependednt & Indepented Variables 
x = data.iloc[:, 0:1].values
y = data.iloc[:, 1]

# Train Test Split
from sklearn.model_selection import train_test_split
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size = 0.33)

# Model Import and Build
from kensu.sklearn.linear_model import LinearRegression

regressor = LinearRegression()
regressor.fit(x_train, y_train)

# Write model
from kensu.pickle import dump
dump(regressor, open('../data/model.pickle', 'wb'))