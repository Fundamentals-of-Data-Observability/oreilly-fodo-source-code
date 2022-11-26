# Credits of original script:
# KRISHNENDU S. KAR on Kaggle
# https://www.kaggle.com/code/ksekhark/simple-linear-regression-using-sklearn/notebook

import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

# Load model
from pickle import load
regressor = load(open('../data/model.pickle', 'rb'))

## Predict on new input
data = pd.read_csv('../data/data.csv')
data["prediction"] = regressor.predict(data.iloc[:,0:1].values)

# Serve result
data.to_csv("../data/predictions.csv")