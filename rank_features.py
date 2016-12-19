
from data_handler import DataHandler
from cust_profile_creator import CustProfileCreator

from collections import defaultdict, Counter

import pyodbc
import pprint  # pretty print.. 
import pandas as pd
import pickle
import time
import os.path
import numpy as np

# machine learning related
import sklearn
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

if __name__ == "__main__":
	
	config_parameters = defaultdict(str)

	# read the configuration file

	try:
		with open("config.info", "r") as f:
			for line in f:
				if ("=" in line) and ("#" not in line):
					tokens = [w.strip() for w in line.split("=")]
					config_parameters[tokens[0]] = tokens[1]
	except IOError as e:
		print("I/O error ({}): {}".format(e.errno, e.sterror))


	pp = pprint.PrettyPrinter(indent=1)
	pp.pprint(config_parameters)
	
	# get table from the TEGA SQL database

	dg = DataHandler(config_parameters)  # create DataHandler object
	dg.download_or_load()  # either download tables or load from local drive
	dg.show_table(4)

	# create customer profile data frame

	fe = CustProfileCreator(dg.dwnl_tbl, config_parameters)
	fe.data_summary()
	fe.show_mosaic_representation()
	fe.show_cust_state_representation()
	fe.show_cust_age_representation()
	fe.create_customer_features()
	print("creating customer profile...")
	fe.create_profile()
	
	print("customers included in the profile belong to the following {} classes:{}".format(len(fe.pops), fe.pops))
	
	# 
	# prediction

	print("you are using scikit-learn version {}...".format(sklearn.__version__))

	# training and testing set
	# # note: splitting so that customers from each population comprise the same proportion in both the training and teting sets

	X_train, X_test, y_train, y_test = train_test_split(fe.customer_profile.loc[:, [c for c in list(fe.customer_profile) if c not in ["Population", "CustomerID"]]],
														 fe.customer_profile.loc[:,"Population"],
								 test_size=0.2, stratify=fe.customer_profile.Population, random_state=113)

	print("created the training and testing sets; the training set contains {} customers and the testing set {} customers...".format(len(X_train.index), len(X_test.index)))
	print("in the training set, each population represented as below:")
	print({fe.pops_inverse_enc[k]: v for k, v in Counter(y_train).items()})
	# print("y_train:",y_train)

	rf_parameters = {'n_estimators': np.arange(1,12,1).tolist(),'min_weight_fraction_leaf':np.arange(0.01,0.5,0.01).tolist()}

	forest = RandomForestClassifier()
	rf_grid = GridSearchCV(forest, rf_parameters)
	print("training random forests...")
	rf_grid.fit(X_train, y_train)
	# print("best parameter values:", rf_grid.best_params_)
	best_forest = rf_grid.best_estimator_
	# best_rf.fit(X_train, y_train)
	print("accuracy score is {}".format(round(accuracy_score(y_test, rf_grid.predict(X_test)), 2)))

	# fimps = sorted( zip(list(X_test), best_forest.feature_importances_), key=lambda x: x[1], reverse=True)[:20]

	# upload_df = pd.DataFrame({"feature":[k for k, v in fimps], "importance":[ "%.3f" % v for k,v in fimps]})
	# upload_df["importance"] = upload_df["importance"].astype(float)

	# # print(upload_df)

	# upload_df.to_csv("./data/importances_df.csv", sep="\t", index=False)

	# # connect using the same details as before
	# conn = pyodbc.connect(dg.auth)
	# cursor = conn.cursor()

	# # first check if the table already exists
	
	
	# fimps_ifexists_drop_quesry = ("IF OBJECT_ID(N'" + config_parameters["TABLE_FEATURE_IMPORTANCES"] + "', N'U') IS NOT NULL BEGIN DROP TABLE " + config_parameters["TABLE_FEATURE_IMPORTANCES"] + " END;")
	# cursor.execute(fimps_ifexists_drop_quesry)

	# fimps_new_table_query = ("CREATE TABLE " + config_parameters["TABLE_FEATURE_IMPORTANCES"] + 
	# 							" (feature varchar(255), importance real);")
	
	# cursor.execute(fimps_new_table_query)
	# print("created a new table called", config_parameters["TABLE_FEATURE_IMPORTANCES"])
	
	# time_now = time.time()
	# print("uploading feature importances to the table...")

	# for row in upload_df.itertuples():

	# 	row_feature = row[1]
	# 	row_importance = row[2]

	# 	add_values_query = ("INSERT INTO " + config_parameters["TABLE_FEATURE_IMPORTANCES"] + " (feature, importance)" + 
	# 										" values (" + "'" + row_feature + "'"  + "," + str(row_importance) + ");")
	# 	cursor.execute(add_values_query)

	# conn.commit()

	# time_after = time.time()

	# print("done. elapsed time {} sec".format(round((time_after-time_now),1)))
	# # bulk_insert_query = ("BULK INSERT " + config_parameters["TABLE_FEATURE_IMPORTANCES"] + 
	# # 							" FROM './data/importances_df.csv' WITH (FIELDTERMINATOR='\\t',ROWTERMINATOR='\\n');")
	# # cursor.execute(bulk_insert_query)
	# # conn.commit()

	# cursor.close()
	# conn.close()








