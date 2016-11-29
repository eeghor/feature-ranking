"""
Data Grabber
"""
import pyodbc
import sys
import pandas as pd
import pickle
import time
import os.path

class DataGrabber(object):

	def __init__(self, pars):

		self.cust_data_tbl = pars["CUST_DATA_TABLE"]
		self.trans_info_tbl =pars["TRANS_INFO_TABLE"]
		self.get_howmany_rows = pars["GET_NROWS"]
		self.pkl_data = pars["TABLE_FILE"]
		self.auth = "DSN=" + pars["DSN"] +";" + "PWD=" + pars["PWD"]
		self.join_tabs_query = ("SELECT c.[CustomerID],"
								"[Gender],[ageGroup],[MosaicType],"
								"c.[state] AS CustomerState,c.[Population] AS CustPop,"
								"s.[Population] AS SalePop,s.mj_tnum AS transID,"
								"[DaysAhead],[ValueAdmitQty],[AdmitQty],"
								"[Sales],[VenueState],s.pk_event_dim,"
								"[CancelledFlag],[BChannel],[MTypePrimary],"
								"[MTypeSecondary],[CardType],[EventNameStandard],"
								"[PrimaryShow],[PrimaryShowDesc],pk_attribute_dim "
								"FROM " + self.cust_data_tbl + " AS c "
								"INNER JOIN " + self.trans_info_tbl + " AS s "
								"ON c.customerid = s.CustomerID")
		self.mosaic_extra_query = ""
		self.mosaic_flag = pars["HANDLE_CUSTOMERS_WITH_NO_MOSAIC_GROUP"]

		self.df = pd.DataFrame()

	def grab(self):

		if os.path.exists(self.pkl_data):

			print("pickled data frame already exists")

			with open(self.pkl_data, "rb") as f:
				self.df = pickle.load(f)

			print("loaded data from file {}".format(self.pkl_data))

		else:

			print("preparing to get data from SQL database...")

			conn = pyodbc.connect(self.auth)

			start_time = time.time()

			if self.mosaic_flag == "0":

				print("ignoring records without Mosaic types...")

				self.mosaic_extra_query = " where [MosaicType] IS NOT NULL"


			if self.get_howmany_rows != "*":

				sql_line = "select top " + str(self.get_howmany_rows) + "*" + " from " + "(" + self.join_tabs_query + ")" + self.mosaic_extra_query + ";"

			else:

				sql_line = "select " + str(self.get_howmany_rows) + " from " + "(" + self.join_tabs_query + ")"  + self.mosaic_extra_query + ";"

			self.df = pd.read_sql(sql_line, conn)
			self.df.to_pickle(self.pkl_data)
			
			print("saved data frame to file {}".format(self.pkl_data))
			
			end_time = time.time()

			print("done. elapsed time: {} minutes".format(round((end_time-start_time)/60,2)))

	def nrows(self):

		print("number of rows in data frame: {}".format(len(self.df.index)))

	def show_variables(self):

		print("data frame contains the following variables:\n")
		for i, v in enumerate(list(self.df)):
			print("[{}]:\t{}".format(i, v))

		