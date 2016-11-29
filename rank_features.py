
import pyodbc
import pandas as pd
import pickle
import time
import os.path
import sklearn
import numpy as np
from data_grabber import DataGrabber
from feature_extractor import CustProfileCreator
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from collections import defaultdict, Counter

if __name__ == "__main__":
	
	config_parameters = defaultdict(str)

	# read the configuration file

	with open("config.info", "r") as f:
		for line in f:
			if "=" in line:
				tokens = [w.strip() for w in line.split("=")]
				config_parameters[tokens[0]] = tokens[1]

	# get table from the TEGA SQL database

	dg = DataGrabber(config_parameters)
	dg.grab()

	# create customer profile data frame

	fe = CustProfileCreator(dg.df, config_parameters)
	fe.describe_input_df()
	fe.create_customer_features()
	fe.create_profile()
	
	print("customers included in the profile we created belong to the following {} populations:{}".format(len(fe.pops), fe.pops))
	
	# 
	# prediction

	print("your scikit-learn version: {}".format(sklearn.__version__))

	# training and testing set
	# note: splitting so that customers from each population comprise the same proportion in both the training and teting sets

	X_train, X_test, y_train, y_test = train_test_split(fe.customer_profile.loc[:, [c for c in list(fe.customer_profile) if c not in ["Population", "CustomerID"]]],
														 fe.customer_profile.loc[:,"Population"],
								 test_size=0.2, stratify=fe.customer_profile.Population, random_state=113)

	print("created the training anf testing sets; the training set contains {} customers and the testing set {} customers...".format(len(X_train.index), len(X_test.index)))
	print("in the training set, we have the following representation for each population:")
	# print({fe.pops_inverse_enc[k]: v for k, v in Counter(y_train).items()})
	# print("y_train:",y_train)

	rf_parameters = {'n_estimators': np.arange(1,6,1).tolist(),'min_weight_fraction_leaf':np.arange(0.01,0.2,0.01).tolist()}

	forest = RandomForestClassifier()
	rf_grid = GridSearchCV(forest, rf_parameters)
	rf_grid.fit(X_train, y_train)
	print("best parameter values:", rf_grid.best_params_)
	best_forest = rf_grid.best_estimator_
	# best_rf.fit(X_train, y_train)
	print("accuracy score is {}".format(round(accuracy_score(y_test, rf_grid.predict(X_test)), 2)))

	fimps = sorted( zip(list(X_test), best_forest.feature_importances_), key=lambda x: x[1], reverse=True)[:20]

	upload_df = pd.DataFrame({"feature":[k for k, v in fimps], "importance":[ "%.2f" % v for k,v in fimps]})

	print(upload_df)

	# connect using the same details as before
	conn = pyodbc.connect(dg.auth)

	upload_df.to_sql(config_parameters["TABLE_FEATURE_IMPORTANCES"], conn, index=False)



