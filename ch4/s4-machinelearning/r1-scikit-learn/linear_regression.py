import sklearn.linear_model as sk_lm

class LinearRegression(sk_lm.LinearRegression):
    def fit(self, X, y, sample_weight=None):
        # 1. call the `sk_lm.LinearRegression` first to compute the fitted model
        model = super(LinearRegression, self).fit(X, y, sample_weight)
        # 2. Compute for the `model`. 
        # E.g., the number of `paramaters`, the min and max of the returned `weight`, etc
        metrics_for(model)
        # 3. Link the `model` to the input data `X` and `y`
        lineage_for(_in=[X, y], _out=[model])
        # 4. Include analytics observations
        # E.g., fetch hyper parameters, execute estimators, ...
        # 5. wrap `model` to ensure its usages will also be observed, and return
        return do_wrap(model)

    def predict(self, X):
        # 1. Compute the predictions first
        predictions = super(LinearRegression, self).predict(X)
        # 2. Wrap the `predictions` to make it data observable
        do_wrap(predictions)
        # 3. link `predictions` to the `model` and the data `X`
        lineage_for(_in=[self, X], _out=[predictions])
        return predictions