
import pyodbc
import pandas as pd
import pickle
import time
import os.path
import sklearn
from sklearn import preprocessing
import numpy as np
from sklearn.feature_selection import VarianceThreshold 
from data_grabber import DataGrabber
from feature_extractor import CustProfileCreator
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from collections import defaultdict

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
	# print(fe.customer_profile.head())

	# # missing val;ues
	# print(fe.customer_profile.count()-len(fe.customer_profile.index))

	# prediction

	print("your scikit-learn version: {}".format(sklearn.__version__))

	# training and testing set
	"""
	X_train, X_test, y_train, y_test = train_test_split(fe.customer_profile.iloc[:,1:], fe.customer_profile.iloc[:,0],
								 test_size=0.2, stratify=fe.customer_profile.Population, random_state=113)

	rf_parameters = {'n_estimators': np.arange(4,50,1).tolist(),'min_weight_fraction_leaf':np.arange(0.01,0.5,0.025).tolist()}
	rf = RandomForestClassifier()
	rf_grid = GridSearchCV(rf, rf_parameters)
	rf_grid.fit(X_train,list(y_train))

	print("accuracy score is {}".format(accuracy_score(y_test, rf_grid.predict(X_test))))

	rf.feature_importances_

	fimps = zip(list(X_test), rf.feature_importances_)
	sorted(fimps, key=lambda x: x[1], reverse=True)
	"""




