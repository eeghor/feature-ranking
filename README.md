# Feature Ranking for User Profiles

The Purpose of this project is to rank features in order of importance as understood by the Random Forests scikit-learn implementation described [here](http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestClassifier.html).

### Pre-requisites
* DSN has to be set up and you have to have access to the SQL transaction profile tables
* Python 3 
* some Python packages such as **pandas** or **scikit-learn** need to be installed (see the imports for more details); typically, all you have to do is to run  

  > pip3 install pandas

### Assumptions
* two transaction profile tables named something like __SalesFacts__ and __CustData__ are assumed to be available in the TEGA database
* customers are identified in these tables by customer IDs and every transaction has a unique transaction ID
* customers belong to populations; what these populations are exactly? anything that sits in the *Population* column in the transaction profile SQL tables; for example, there may be *customer who is a client of the xxx company* and *customer who is not a client of this company*
* any customer may belong to a number of populations; for example, if the populations are *Australian Open 2010 attendees* and *Australian Open 2016 attendees* there is likely to be a group of customers who have attended both events

### What we want to do
We are interested in figuring out what defines the **differences** between the populations most. 

### Approach
First of all, instead of dealing with the transactions as such we would like to handle the data at the customer level. This means that based on the transaction descriptions available in the tables we work with we effectively create customer profiles: for each customer, we introduce a number of so-called features; an example of a feature: *customer purchased at least one opera ticket within last 6 month*. 

Note: the features have to be generic and applicable to any populations.

Once the data frame containing customer records and the corresponding features has been created, we use the Random Forests classifier to sort customers across populations. Due to how Random Forests work, once the best possible classification accuracy has been achieved we can extract the feature importances. We sort these, take a reasonable number of most important ones and put them in a table. 

<img src="/pics/ranker_scheme.png" width="1000"/>

#### Note
Feature importance should be understood as sometihng along the following lines: does having a particular feature makes difference in terms of the achieved classification accuracy? Some features can make a lot less difference than others and then we consider them unimportant. Explanation of **why** importance is a separate question.

