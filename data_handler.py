"""
Data Grabber
"""
import pyodbc
import sys
import pandas as pd
import pickle
import time
import os.path
from collections import defaultdict

class DataHandler(object):

	def __init__(self, pars):

		self._cust_tbl = pars["CUST_DATA_TABLE"]
		self._tran_tbl = pars["TRANS_INFO_TABLE"]
		self._nrow_get = pars["GET_NROWS"]
		self._nrow = 0  # how many rows o get
		self._tran_pkl = pars["TABLE_FILE"]  # to pickle downloaded table
		self._auth = "DSN=" + pars["DSN"] +";" + "PWD=" + pars["PWD"]
		self.join_tabs_query = ("SELECT c.[CustomerID],"
								"[Gender],[ageGroup],[MosaicType],"
								"c.[state] AS CustomerState,c.[Population] AS CustPop,"
								"s.[Population] AS SalePop,s.mj_tnum AS transID,"
								"[DaysAhead],[ValueAdmitQty],[AdmitQty],"
								"[Sales],[VenueState],s.pk_event_dim,"
								"[CancelledFlag],[BChannel],[MTypePrimary],"
								"[MTypeSecondary],[CardType],[EventNameStandard],"
								"[PrimaryShow],[PrimaryShowDesc],pk_attribute_dim, [transactionDate] "
								"FROM " + self._cust_tbl + " AS c "
								"INNER JOIN " + self._tran_tbl + " AS s "
								"ON c.customerid = s.CustomerID")
		
		self._mosa_flg = pars["HANDLE_CUSTOMERS_WITH_NO_MOSAIC_GROUP"]
		self._mosa_add_qry = ""
		self._enf_down = pars["ENFORCE_DOWNLOAD"].lower().strip()
		self.dwnl_tbl = pd.DataFrame()
		self._dwl_time = 0
		self._vexpl = defaultdict(str)

		with open("var_explanation.txt", "r") as f:
			for line in f:
				var, expl = [v.strip() for v in [line[:line.index(":")], line[line.index(":")+1:]]]
				self._vexpl[var] = expl

	#
	# get a table using the supplied SQL string then put it in a data frame and save this data frame 
	# locally as a pickle; 
	# use this function when the data we are after is not available locally or it is but you still prefer
	# to download it again and rewrite the pickle file
	#

	def _sql_to_df(self, sql_string):

		start_time = time.time()
		print("setting up database connection...", end="")
				
		conn = pyodbc.connect(self._auth)
		print("ok")
		
		print("reading SQL table into data frame...", end="")
		df = pd.read_sql(sql_string, conn)
		print("ok")
		print("downloaded rows...", end="")
		print(len(df.index))
		print("pickling...", end="")
		df.to_pickle(self._tran_pkl)
		print("ok")
		end_time = time.time()
		
		tm = round((end_time-start_time)/60,1)  # elapsed time in minutes

		return (df, tm)


	#
	# finalise the query to be sent to join the customer and transaction info tables
	#	

	def _create_query(self):

		_mosa_add_qry = ""

		if self._mosa_flg == "0":  # if ignore people with no known Mosaic group
			_mosa_add_qry = " where k.[MosaicType] IS NOT NULL"

		# if no need to get all rows
		if self._nrow_get != "*":

			extra_bit = " top " + str(self._nrow_get) + " * "
		else:
			extra_bit = " * "
			
		sql_line = ("select " + extra_bit + " from (" + self.join_tabs_query + 
														") as k" + self._mosa_add_qry + ";")
 
		return sql_line

	# 
	# decide if downloading the table is needed
	#

	def download_or_load(self):

		if self._enf_down == "yes":

			# delete file if exists
			if os.path.exists(self._tran_pkl):
				os.remove(self._tran_pkl)
			
			# attempt download
			self.dwnl_tbl, self._dwl_time = self._sql_to_df(self._create_query())


		elif self._enf_down == "no":

			# if file exists, load data frame from there
			if os.path.exists(self._tran_pkl):
				
				with open(self._tran_pkl, "rb") as f:
					self.dwnl_tbl = pickle.load(f)
					print("loaded data from local pickle...")
			else:
				# if there's no file, attempt donwload
				self.dwnl_tbl, self._dwl_time = self._sql_to_df(self._create_query())

		self._nrow = len(self.dwnl_tbl.index)

	#
	# preview the data frame created from that table
	#

	def show_table(self, n=10):

		print(self.dwnl_tbl.head(n))
		print("---> summary on downloaded table")
		print("rows...{}".format(self._nrow))
		print("columns...{}".format(self.dwnl_tbl.shape[1]))
		print("unique customers...{}".format(len(list(self.dwnl_tbl["CustomerID"].unique()))))
		print("unique transactions...{}".format(len(list(self.dwnl_tbl["transID"].unique()))))

		for v in list(self.dwnl_tbl):
			try:
				print("{}\t: {}".format(v, self._vexpl[v]))
			except:

				print("{}\t: {}".format(v, "not sure what it is.."))
				pass

					
					
		