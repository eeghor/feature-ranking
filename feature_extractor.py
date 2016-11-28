"""
This class takes a data frame that is effectively the original TRANSACTION profile table and produces a CUSTOMER profile 
data frame;

INPUT:  the input data frame has the following columns:

'CustomerID', 'Gender', 'ageGroup', 'MosaicType', 'CustomerState', 'CustPop', 'SalePop', 
'transID', 'DaysAhead', 'ValueAdmitQty', 'AdmitQty', 'Sales', 'VenueState', 'pk_event_dim', 
'CancelledFlag', 'BChannel', 'MTypePrimary', 'MTypeSecondary', 'CardType', 'EventNameStandard', 
'PrimaryShow', 'PrimaryShowDesc', 'pk_attribute_dim'

OUTPUT: a data frame containing features for the customers, that is, it has the form

'CustomerID', 'Feature 1', 'Feature 2', .... 

--- igor k.  --- 28NOV2016 ---

"""

import pandas as pd
import pickle
from collections import defaultdict, Counter
import re

class CustProfileCreator(object):

	def __init__(self, transaction_df):

		# drop duplicates on CustomerID and transaction ID (the same customer and transaction can be in several populations)
		self.df = transaction_df.drop_duplicates(subset=["CustomerID", "transID"], inplace=False)
		self.noriginal_trans = len(transaction_df.index)
		self.ucustomer_ids = list(self.df["CustomerID"].unique())  # list of unique customer IDs
		self.nuc = len(self.ucustomer_ids)
		self.cust_feature_dict = defaultdict(lambda: defaultdict(int))  # {"customerID1": {"feature1": 1, "feature2": 0, ..}, ..}
		self.popular_sec_mtypes = sorted([(k,v) for k,v in Counter(self.df["MTypeSecondary"]).items() 
													if k.isalnum()], key=lambda x: x[1], reverse=True)[:20]
		self.list_popular_sec_mtypes = [tp for tp, co in self.popular_sec_mtypes]
		self.customer_profile = pd.DataFrame()

		# intermediate features:
		self.cust_mtype_counts = defaultdict(lambda: defaultdict(int))
		self.cust_pmtype_counts = defaultdict(lambda: defaultdict(int))
		self.cust_mosaic = defaultdict(lambda: defaultdict(int))

	def describe_input_df(self):

		print("input data frame contains {} transaction records in total; {} unique transactions by {} unique customers".format(self.noriginal_trans, len(self.df.index), self.nuc))
		print("popular secondary mtypes are {}".format(self.list_popular_sec_mtypes))


	def approve_feature(self, col):

		list_whats_in_column = list(Counter(col).keys())

		if len(list_whats_in_column) > 1:
				print("warning! this customer belongs to multiple classes meant to be mutually eclusive!")

		if len(list_whats_in_column) > 0 and (list_whats_in_column[0] is not None):

			return (True, list_whats_in_column[0])

		else:

			return (False, list_whats_in_column[0])

	def create_customer_features(self):
	
		for customer in self.ucustomer_ids:
			
			# create a data frame containing stansactions only for this customer
			df_only_this_customer = self.df.loc[self.df["CustomerID"] == customer]

			#print("customer is now", customer)
			#print("secondary types for this customer are ", Counter(df_only_this_customer["MTypeSecondary"]).items() )
			self.cust_mtype_counts[customer] = {k: v for k,v in Counter(df_only_this_customer["MTypeSecondary"]).items() 
															if k in self.list_popular_sec_mtypes}  # note: count only popular secondary mtypes

			self.cust_pmtype_counts[customer] = Counter(df_only_this_customer["MTypePrimary"])
			
			tmp_mostypes = list(Counter(df_only_this_customer["MosaicType"]).keys())  # just in case someone is in multiple Mosaic classes

			if len(tmp_mostypes) > 1:
				print("warning! customer with id {} is in multiple Mosaic classes: {}".format(customer, tmp_mostypes))

			# given the Mosaic class moscl for a customer, create customer features based on the class description;
			# a proper Mosaic class looks like [letter][digit1][digit2], for example, "A02"
			# note: there are 49 Mosaic classes in total, hence "<50" when doing sanity check
			
			#
			# collect Mosaic features
			#
			if len(tmp_mostypes) > 0 and (tmp_mostypes[0] is not None):

				mosaic_mask = re.compile('(^[A-M]{1})(\d{2}$)')
				match_res = mosaic_mask.match(tmp_mostypes[0])  # match objects always have a boolean value of True
				assert match_res, "error! this is not a Mosaic group name.." 
				mos_letter = match_res.group(1)  # mosaic letter
				# print("mosaic letter is ", mos_letter)
				mosn = int(match_res.group(2))  # mosaic number
				assert (mosn < 50), "error! the Mosaic class number should be under 50..."
				
				# mosaic letter is a feature:
				self.cust_feature_dict[customer]["mos_letter_" + mos_letter] = 1
		
				# income level features:		
				if (mos_letter in ["A","D"] or 					    # all A and D are rich 
					(mos_letter == "B" and mosn in range(5,9)) or   # B05 to B08 are rich but B09 aren't ("simple needs")
					(mos_letter == "C" and mosn in [10, 12, 13]) or # C11 and C14 are likely to have average income
					(mos_letter == "E" and mosn in [17,18]) or  	# E18 and E19 are probably not that rich
					(mos_letter == "F" and mosn in [21])):  		# F22 to F24 may have average income
					self.cust_feature_dict[customer]["high_income"] = 1

				elif ((mos_letter in ["B"] and mosn in [9]) or      # these are "the good life" older couples
					(mos_letter in ["G","H"]) or
					(mos_letter == "C" and mosn in [11]) or   		# educated singles and couples in early career "inner city aspirations" 
					(mos_letter == "E" and mosn in [19,20])):
					self.cust_feature_dict[customer]["average_income"] = 1

				else:
					self.cust_feature_dict[customer]["low_income"] = 1

				# education features:
				if ((mos_letter in ["A","B","C", "I"]) or 
					(mos_letter == "H" and mosn in [30])):
					self.cust_feature_dict[customer]["good_education"]

				elif ((mos_letter in ["D","F"]) or
					(mos_letter == "H" and mosn in [31,32])):
					self.cust_feature_dict[customer]["average_education"] = 1

				else:
					self.cust_feature_dict[customer]["poor_education"] = 1

			# 
			# collect primary Mtype features
			#
			for pmt in self.cust_pmtype_counts[customer].keys():
				self.cust_feature_dict[customer][pmt] = 1 

			# 
			# collect secondary MType features
			#
			for smt in self.cust_mtype_counts[customer].keys():
				self.cust_feature_dict[customer][smt] = 1 

			#
			# collect age group feature
			#

			flag, val = self.approve_feature(df_only_this_customer["ageGroup"])
			if flag:
				self.cust_feature_dict[customer]["age_group=" + val] = 1

			# 
			# collect gender feature
			#

			flag, val = self.approve_feature(df_only_this_customer["Gender"])
			if flag:
				self.cust_feature_dict[customer]["gender=" + val] = 1

			# 
			# collect customer state features
			#

			flag, val = self.approve_feature(df_only_this_customer["CustomerState"])
			if flag:
				self.cust_feature_dict[customer]["cust_state=" + val] = 1

	def create_profile(self):

		self.customer_profile = pd.DataFrame.from_dict(self.cust_feature_dict, orient="index")
		# deal with missing values where possible

		print("created a customer profile for {} customers; total number of features is {}".format(len(self.customer_profile.index), 
																						len(list(self.customer_profile))))
		self.customer_profile.to_pickle("saved_profile.pkl")
		print("saved profile to file {}".format("saved_profile.pkl"))




