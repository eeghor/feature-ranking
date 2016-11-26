import pandas as pd
import pickle
from collections import defaultdict, Counter
import re

class Feature_Extractor(object):

	def __init__(self, customer_df):

		self.df = customer_df
		self.customer_features = defaultdict(int)
		self.cust_mtype_counts = defaultdict(lambda _: defaultdict(int))
		self.cust_pmtype_counts = defaultdict(lambda _: defaultdict(int))
		self.cust_mosaic = defaultdict(lambda _: defaultdict(int))

	def get_mosaic_features(self, cust_mosaic_class):
	
	# given the Mosaic class cust_mosaic_class, create some features based on the class description;
	# a proper Mosaic class looks like [letter][digit1][digit2];
	# note: there are 49 Mosaic classes in total
		
		if cust_mosaic_class:

			mos = re.compile('(^[A-M]{1})(\d{2}$)')
			match_res = mos.match(cust_mosaic_class)  # match objects always have a boolean value of True
			
			assert match_res, "sorry, this is not a Mosaic group name.."   # error if no match 

			mos_letter = match_res.group(1)  # the result of getting a group is a single string
			mosn = int(match_res.group(2))

			assert (mosn < 50), "Mosaic class number should be under 50..."
			
			#
			# mosaic letter feature:
			#
			self.customer_features["mosaic_letter_" + mos_letter] = 1
			
			#
			# income level feature:
			#
			if (mos_letter in ["A","D"] or 					    # all A and D are rich 
				(mos_letter == "B" and mosn in range(5,9)) or   # B05 to B08 are rich but B09 aren't ("simple needs")
				(mos_letter == "C" and mosn in [10, 12, 13]) or # C11 and C14 are likely to have average income
				(mos_letter == "E" and mosn in [17,18]) or  	# E18 and E19 are probably not that rich
				(mos_letter == "F" and mosn in [21])):  		# F22 to F24 may have average income
				self.customer_features["high_income"] = 1

			elif ((mos_letter in ["B"] and mosn in [9]) or      # these are "the good life" older couples
				(mos_letter in ["G","H"]) or
				(mos_letter == "C" and mosn in [11]) or   		# educated singles and couples in early career "inner city aspirations" 
				(mos_letter == "E" and mosn in [19,20])):
				self.customer_features["average_income"] = 1

			else:
				self.customer_features["low_income"] = 1

			#
			# education feature:
			#
			if ((mos_letter in ["A","B","C", "I"]) or 
				(mos_letter == "H" and mosn in [30])):
				self.customer_features[""]
			elif ((mos_letter in ["D","F"]) or
				(mos_letter == "H" and mosn in [31,32])):
				self.customer_features["average_education"] = 1
			else:
				self.customer_features["poor_education"] = 1

		else:

			pass


	def get_mtype_features(self, mtype_counts):

	# given the mtype_counts dictionary, summarise these in a meaningful way

		if "BALLET" in mtype_counts:
			self.customer_features["likes_ballet"] += 1

		if ("80SPOP" in mtype_counts) or ("80SROCK" in mtype_counts):
			self.customer_features["likes_80s_music"] += 1

		if ("70SPOP" in mtype_counts) or ("70SROCK" in mtype_counts):
			self.customer_features["likes_70s_music"] += 1

		if (("RNB" in mtype_counts) or 
			("SOUL" in mtype_counts) or 
			("RAP" in mtype_counts) or
			("POP" in mtype_counts)):
			self.customer_features["likes_soul_rnb_rap"] += 1

		if (("SENIOR" in mtype_counts) or 
			("COMMUNIT" in mtype_counts)):
			self.customer_features["likes_senior_community_events"] += 1

		if (("SFFILM" in mtype_counts) or 
			("FESTFILM" in mtype_counts) or 
			("CINEMA" in mtype_counts)):
			self.customer_features["likes_movies"] = +1

		if ("GOLF" in mtype_counts):
			self.customer_features["likes_golf"] += 1

		if ("AFL" in mtype_counts):
			self.customer_features["likes_afl"] += 1

		if ("CRICKET" in mtype_counts):
			self.customer_features["likes_cricket"] += 1

		if "HORSER" in mtype_counts:
			self.customer_features["likes_horse_racing"] += 1

		if "SOCCER" in mtype_counts:
			self.customer_features["likes_soccer"] += 1

		if (("RUNION" in mtype_counts) or 
			("LEAGUE" in mtype_counts)):
			self.customer_features["likes_rugby"] += 1

		if "TENNIS" in mtype_counts:
			self.customer_features["likes_tennis"] += 1

		if ("MUSEUM" in mtype_counts):
			self.customer_features["likes_museums"] += 1

	def show_features(self):

		print("currently, the features are as below:")
		for feature, v in self.customer_features.items():
			print("{}\t{}".format(feature, v))

	def process_df(self):

		# create a dictionary with the count of secondary MTypes for each customer;
		# the result should be {"customer1": {"AFL":12, "ballet":1,..}, "customer2": {..}}

		lst_unique_customerIDs = list(self.df["CustomerID"].unique())
		# print(lst_unique_customerIDs)

		for customer in lst_unique_customerIDs:
			 self.cust_mtype_counts[customer] = Counter(self.df.loc[self.df["CustomerID"] == customer, "MTypeSecondary"])
			 # print("counter:",self.cust_mtype_counts[customer])

		# same for the primary type counts

		for customer in lst_unique_customerIDs:
			 self.cust_pmtype_counts[customer] = Counter(self.df.loc[self.df["CustomerID"] == customer, "MTypePrimary"])

		# collect customer Mosaic classes; we use Counter to be able to easily check whether eachcustomer is indeed in 
		# only one Mosaic class

		for customer in lst_unique_customerIDs:
			self.cust_mosaic[customer] = Counter(self.df.loc[self.df["CustomerID"] == customer, "MosaicType"])
			# print("mosaic for this customer:", self.cust_mosaic[customer])
			if len(self.cust_mosaic[customer]) > 1:
				print("note: customer with ID={} is in multiple Mosaic classes: {}".format(customer, list(self.cust_mosaic[customer].keys())))

		for customer in self.cust_mosaic:
		# now create the MType features
			self.get_mtype_features(self.cust_mtype_counts[customer])
		# and Mosaic features
			cust_moss = list(self.cust_mosaic[customer].keys())
			# print(cust_moss)
			self.get_mosaic_features(cust_moss[0])


