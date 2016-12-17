"""
This class takes a data frame that is effectively the original TRANSACTION profile table and produces a CUSTOMER profile 
data frame;

INPUT:  the input data frame has the following columns:

OUTPUT: a data frame containing features for the customers, that is, it has the form

'CustomerID', 'Feature 1', 'Feature 2', .... 

--- igor k.  --- 28NOV2016 ---

"""

import pandas as pd
import pickle
from collections import defaultdict, Counter
import re

class CustProfileCreator(object):

	def __init__(self, transaction_df, pars):

		# IMPORTANT! drop duplicates on CustomerID and transaction ID because the same customer and transaction can be 
		# in several customer populations; with keep=False the rows related to customers belonging to multiple populations 
		# are removed COMPLETELY

		self.df = transaction_df.drop_duplicates(subset=["CustomerID", "transID"], keep=False, inplace=False)
		

		#self.noriginal_trans = len(transaction_df.index)
		self.ucustomer_ids = list(self.df["CustomerID"].unique())  # list of unique customer IDs
		self.pops = set(self.df["CustPop"]) | set(self.df["SalePop"]) 
		
		self.pops_enc = dict(zip(self.pops, range(1,len(self.pops) + 1)))
		self.pops_inverse_enc = {v: k for k, v in self.pops_enc.items()}
		
		#self.nuc = len(self.ucustomer_ids)
		self.cust_feature_dict = defaultdict(lambda: defaultdict(int))  # {"customerID1": {"feature1": 1, "feature2": 0, ..}, ..}
		self.customer_profile = pd.DataFrame()
		
		# keep sets of features by type
		self.mosaic_letter_features = set()
		self.mosaic_income_features = set()
		self.mosaic_education_features = set()
		self.age_features = set()
		self.gender_features = set()
		self.customer_state_features = set()
		self.mtype_primary_features = set()
		self.mtype_secondary_features = set()
		self.pop_features = set()

		# junk values
		self.mtype_secondary_junk = pars["MTYPE_SECONDARY_JUNK"].split()
		self.mtype_primary_junk = pars["MTYPE_PRIMARY_JUNK"].split()

		self.popular_sec_mtypes = sorted([(k,v) for k,v in Counter(self.df["MTypeSecondary"]).items() 
													if (k.isalnum() and k not in self.mtype_secondary_junk)], key=lambda x: x[1], reverse=True)[:int(pars["NTOP_SEC_MTYPES_INTO_FEATURES"])]
		self.list_popular_sec_mtypes = [tp for tp, co in self.popular_sec_mtypes]
		self.mosaic_flag = pars["HANDLE_CUSTOMERS_WITH_NO_MOSAIC_GROUP"]
		self.gender_flag = pars["HANDLE_CUSTOMERS_WITH_NO_GENDER"]
		self.savetofile = pars["CUST_PROF_FILE"]

		# intermediate features:
		self.cust_mtype_counts = defaultdict(lambda: defaultdict(int))
		self.cust_pmtype_counts = defaultdict(lambda: defaultdict(int))
		self.cust_mosaic = defaultdict(lambda: defaultdict(int))
		self.cust_pop_counts = defaultdict(lambda: defaultdict(int))

		# population by state, ABS; 2016
		self.AU_state_pops_Ks = {"NSW": 7704.3, "VIC": 6039.1, "QLD": 4827.0, "SA": 1706.5, "WA": 2613.7, "TAS": 518.5, "NT": 244.0, "ACT": 395.2}

	def data_summary(self):

		print("---> some data info")
		print("number or rows: {}".format(len(self.df.index)))
		print("unique customers: {}".format(len(self.ucustomer_ids)))
		print("unique transactions: {}".format(len( list(self.df["transID"].unique()))))
		print("populations: {}".format(len(self.pops)))

		gc = Counter(self.df["Gender"])  # gender counts

		print("males: {}% females: {}% no gender: {}%".format( *map(lambda _: round(100* _/sum(gc.values()),1), 
			[gc["M"], gc["F"], sum(gc.values()) - gc["M"] - gc["F"]])))

		print("customers with known Mosaic type: {}%".format(round(sum(self.df["MosaicType"].notnull())/len(self.df.index)*100,2) )
)
		missings = self.df.count() - len(self.df.index)
		print("missing values:\n", missings[missings < 0])
		

	def _decompose_mosaic(self, mostype):
	
		mosaic_mask = re.compile('(^[A-M]{1})(\d{2}$)')
		
		match_res = mosaic_mask.match(mostype)  # match objects always have a boolean value of True
		assert match_res, "error! this is not a Mosaic group name.." 
		
		mos_letter = match_res.group(1)  # mosaic letter
		mosn = int(match_res.group(2))  # mosaic number
		assert (mosn < 50), "error! the Mosaic class number should be under 50..."	

		return (mos_letter, mosn)

	def show_mosaic_representation(self):

		# extract letters only
		ccc = Counter(self.df["MosaicType"].loc[self.df["MosaicType"].notnull()].apply(lambda _: self._decompose_mosaic(_)[0], 1))
		count_all_letters = sum(ccc.values())

		letter_ranking = sorted([(k,round(v*100/count_all_letters,1)) for k,v in ccc.items()], key=lambda x: x[1], reverse=True)

		print("---> customers by mosaic letters")
		print("{}\t{}".format("  ","%"))
		for t in letter_ranking:
			print("{}:\t{}".format(*t))

	def show_cust_state_representation(self):

		stt = Counter(self.df["CustomerState"].loc[self.df["CustomerState"].notnull()])
		count_all_states = sum(stt.values())

		state_ranking = sorted([(k,round(v*100/count_all_states,1)) for k,v in stt.items()], key=lambda x: x[1], reverse=True)
		print("---> customers by state")
		print("{}\t{}".format("  ","%"))
		for t in state_ranking:
			print("{}:\t{}".format(*t))
		
		state_ranking_pops = sorted([(k,round(v*100/(self.AU_state_pops_Ks[k]*1000),2)) for k,v in stt.items()], key=lambda x: x[1], reverse=True)
		
		print("---> customers by state as % of state population (2016)")
		for t in state_ranking_pops:
			print("{}:\t{}".format(*t))


	def show_cust_age_representation(self):

		agc = Counter(self.df["ageGroup"].loc[self.df["ageGroup"].notnull() & ~self.df["ageGroup"].isin(["UNK"])])
		count_all_ages = sum(agc.values())

		age_ranking = sorted([(k,round(v*100/count_all_ages,1)) for k,v in agc.items()], key=lambda x: x[1], reverse=True)
		
		print("---> customers by age")
		print("{}\t{}".format("  ","%"))
		for t in age_ranking:
			print("{}:\t{}".format(*t))

	
	def _approve_feature(self, col):

		list_whats_in_column = list(Counter(col).keys())

		if len(list_whats_in_column) > 1:
				print("warning! this customer belongs to multiple classes meant to be mutually eclusive!")

		if len(list_whats_in_column) > 0 and (list_whats_in_column[0] is not None) and (list_whats_in_column[0] != "UNK"):

			return (True, list_whats_in_column[0])

		else:

			return (False, list_whats_in_column[0])

	def create_customer_features(self):
	
		for customer in self.ucustomer_ids:
			
			# create a data frame containing stansactions only for this customer
			df_only_this_customer = self.df.loc[self.df["CustomerID"] == customer]

			self.cust_mtype_counts[customer] = {k: v for k,v in Counter(df_only_this_customer["MTypeSecondary"]).items() 
															if k in self.list_popular_sec_mtypes}  # note: count only popular secondary mtypes

			self.cust_pmtype_counts[customer] = Counter(df_only_this_customer["MTypePrimary"])
			
			tmp_mostypes = list(Counter(df_only_this_customer["MosaicType"]).keys())  # just in case someone is in multiple Mosaic classes

			self.cust_pop_counts[customer] = Counter(df_only_this_customer["CustPop"]) + Counter(df_only_this_customer["SalePop"])

			if len(tmp_mostypes) > 1:
				print("warning! customer with id {} is in multiple Mosaic classes: {}".format(customer, tmp_mostypes))

			# given the Mosaic class moscl for a customer, create customer features based on the class description;
			# a proper Mosaic class looks like [letter][digit1][digit2], for example, "A02"
			# note: there are 49 Mosaic classes in total, hence "<50" when doing sanity check
			
			#
			# collect Mosaic features
			#
			if len(tmp_mostypes) > 0 and (tmp_mostypes[0] is not None):

				# mosaic_mask = re.compile('(^[A-M]{1})(\d{2}$)')
				# match_res = mosaic_mask.match(tmp_mostypes[0])  # match objects always have a boolean value of True
				# assert match_res, "error! this is not a Mosaic group name.." 
				# mos_letter = match_res.group(1)  # mosaic letter
				# # print("mosaic letter is ", mos_letter)
				# mosn = int(match_res.group(2))  # mosaic number
				# assert (mosn < 50), "error! the Mosaic class number should be under 50..."
				
				mos_letter, mosn = self._decompose_mosaic(tmp_mostypes[0])
				# mosaic letter is a feature:
				self.cust_feature_dict[customer]["mos_letter_" + mos_letter] = 1
				self.mosaic_letter_features.add("mos_letter_" + mos_letter)
		
				# income level features:		
				if (mos_letter in ["A","D"] or 					    # all A and D are rich 
					(mos_letter == "B" and mosn in range(5,9)) or   # B05 to B08 are rich but B09 aren't ("simple needs")
					(mos_letter == "C" and mosn in [10, 12, 13]) or # C11 and C14 are likely to have average income
					(mos_letter == "E" and mosn in [17,18]) or  	# E18 and E19 are probably not that rich
					(mos_letter == "F" and mosn in [21])):  		# F22 to F24 may have average income
					self.cust_feature_dict[customer]["high_income"] = 1
					self.mosaic_income_features.add("high_income")

				elif ((mos_letter in ["B"] and mosn in [9]) or      # these are "the good life" older couples
					(mos_letter in ["G","H"]) or
					(mos_letter == "C" and mosn in [11]) or   		# educated singles and couples in early career "inner city aspirations" 
					(mos_letter == "E" and mosn in [19,20])):
					self.cust_feature_dict[customer]["average_income"] = 1
					self.mosaic_income_features.add("average_income")

				else:
					self.cust_feature_dict[customer]["low_income"] = 1
					self.mosaic_income_features.add("low_income")

				# education features:
				if ((mos_letter in ["A","B","C", "I"]) or 
					(mos_letter == "H" and mosn in [30])):
					self.cust_feature_dict[customer]["good_education"] = 1
					self.mosaic_education_features.add("good_education")

				elif ((mos_letter in ["D","F"]) or
					(mos_letter == "H" and mosn in [31,32])):
					self.cust_feature_dict[customer]["average_education"] = 1
					self.mosaic_education_features.add("average_education")

				else:
					self.cust_feature_dict[customer]["poor_education"] = 1
					self.mosaic_education_features.add("poor_education")

			# 
			# collect primary Mtype features
			#
			for pmt in self.cust_pmtype_counts[customer].keys():
				if pmt not in self.mtype_primary_junk:
					self.cust_feature_dict[customer][pmt] = 1
					self.mtype_primary_features.add(pmt)

			# 
			# collect secondary MType features
			#
			for smt in self.cust_mtype_counts[customer].keys():
				self.cust_feature_dict[customer][smt] = 1 
				self.mtype_secondary_features.add(smt)

			#
			# collect age group feature
			#

			flag, val = self._approve_feature(df_only_this_customer["ageGroup"])
			if flag:
				ag_feature = "age_group=" + val
				self.cust_feature_dict[customer][ag_feature] = 1
				self.age_features.add(ag_feature)

			# 
			# collect gender feature
			#

			if self.gender_flag != "0":
				flag, val = self._approve_feature(df_only_this_customer["Gender"])
				if flag:
					gend_feature = "gender=" + val
					self.cust_feature_dict[customer][gend_feature] = 1
					self.gender_features.add(gend_feature)


			# 
			# collect customer state features
			#

			flag, val = self._approve_feature(df_only_this_customer["CustomerState"])
			if flag:
				cstate_feature = "cust_state=" + val
				self.cust_feature_dict[customer][cstate_feature] = 1
				self.customer_state_features.add(cstate_feature)

			#
			# temporal sales features: we look into the purchases during last 12 months from NOW if available
			#

			



			# 
			# collect population features
			#
			# for j in range(len(self.cust_pop_counts[customer].keys()),1,-1):
			# 	if j:
			# 		pop_feature = "in_" + str(j) + "_pops"
			# 		self.pop_features.add(pop_feature)
			# 		self.cust_feature_dict[customer][pop_feature] = 1 
			
			if len(self.cust_pop_counts[customer].keys()) == 1:  # if only in one population
				for k in self.cust_pop_counts[customer].keys():
					self.cust_feature_dict[customer]["Population"] = self.pops_enc[k]
			
	def create_profile(self):

		# first create a data frame

		self.customer_profile = pd.DataFrame.from_dict(self.cust_feature_dict, orient="index")
		self.customer_profile["CustomerID"] = self.customer_profile.index

		# print("customer df has the following columns:", list(self.customer_profile))
		# do we need to get rid of some customers? 

		# idx_customers_remove = [cid for cid in self.cust_pop_counts.keys() if len(self.cust_pop_counts[cid].keys()) > 1]
		# print("turns out that {} cutomers are in multiple populations!".format(len(idx_customers_remove)))

		# self.customer_profile = self.customer_profile[~self.customer_profile["CustomerID"].isin(idx_customers_remove)]

		# deal with missing values where possible

		# fill the below features with zeros where the values are missing

		idx_missing_zero = self.mtype_primary_features | self.mtype_secondary_features | self.customer_state_features | self.pop_features | self.age_features | self.mosaic_letter_features | self.mosaic_income_features | self.mosaic_education_features

		self.customer_profile.loc[:,idx_missing_zero] = \
		self.customer_profile.loc[:,idx_missing_zero].fillna(0)
		
		print("created a customer profile for {} customers; total number of features is {}...".format(len(self.customer_profile.index), 
																						len(list(self.customer_profile))))
		
		self.customer_profile.to_pickle(self.savetofile )
		print("saved profile to file {}...".format(self.savetofile ))




