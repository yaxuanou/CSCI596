# CSCI596
csci596 final project
# Restaurants Recommendation Based on Item-based collaborative filtering 
The GitHub repository for the CSCI596 final project. We will learn and use spark to implement our project.

## Group Members

    Zifan Huang
    Yaxuan Ou
    Jingji Zang

## Spark

Spark is a fast, general-purpose computing engine designed for large-scale data processing. Apache Spark has its architectural foundation in the resilient distributed dataset (RDD), a read-only multiset of data items distributed over a cluster of machines, that is maintained in a fault-tolerant way. The Dataframe API was released as an abstraction on top of the RDD, followed by the Dataset API.

Spark is an open-source parallel framework like Hadoop MapReduce developed by UC Berkeley AMP Lab (AMP Lab of University of California, Berkeley)

![Spark](https://github.com/yaxuanou/CSCI596/blob/main/PNG/spark.png)
## Item-based CF

Collaborative Filtering (CF) is a commonly used algorithm that is available on a variety of e-commerce sites. CF algorithms include user-based CF and item-based CF. We use item-based CF to implement restaurants recommendations.

Item-based collaborative filtering is the recommendation system to use the similarity between items ratings provided by users. In this project, we use Yelp database to calculate business similarity and predict users' rating on the business for doing the recommendation.

![item based](https://github.com/yaxuanou/CSCI596/blob/main/PNG/1.png)

## Work flow of our final project (Implementation)

### Datasets

We generated the following two datasets from the original Yelp review dataset with some filters. We randomly took 60% of the data as the training dataset, 20% of the data as the validation dataset, and 20% of the data as the testing dataset.

1. yelp_train.csv: The training data, which only include the columns: user_id, business_id, and stars. 

2. yelp_val.csv: the validation data, which are in the same format as training data.
 
3. review_train.json: review data only for the training pairs (user, business)

4. user.json: all user metadata

5. business.json: all business metadata, including locations, attributes, and categories 

6. checkin.json: user check-ins for individual businesses

7. tip.json: tips (short reviews) written by a user about a business

8. photo.json: photo data, including captions and classifications

### Calculate Similarity

There are many ways to calculate similarity . We choose Pearson Similarity

<img src="https://github.com/yaxuanou/CSCI596/blob/main/PNG/personal.png" width=50% height=50%>

The similarity between these users can be converted into a similarity matrix

Pearson correlation for w1,W2 Similarity between items1 and times2

<img src="https://github.com/yaxuanou/CSCI596/blob/main/PNG/w.png" width=50% height=50%>

Then wen can predict users rating on businesses. Using this prediction, we can recommend businesses to user.

<img src="https://github.com/yaxuanou/CSCI596/blob/main/PNG/2.png" width=50% height=50%>


### Result
We divide the absolute differences into 5 levels and count the number for each level as following

```
RMSE : 0.9798099093547139
Error Distribution

>=0 and <1: 41286 
>=1 and <2: 15244 
>=2 and <3: 5260
>=3 and <4: 795
>=4: 1
```

This way we will be able to know the error distribution of our predictions and to improve the performance of our recommendation systems.

Additionally, we compute the RMSE (Root Mean Squared Error) by using following formula:

<img src="https://github.com/yaxuanou/CSCI596/blob/main/PNG/RMSE.png" width=50% height=50%>

Where ​Predi​ is the prediction for business ​i and ​Rate ​i is the true rating for business ​i​. n is the total number of the business you are predicting.

<img src="https://github.com/yaxuanou/CSCI596/blob/main/PNG/result.jpg" width=50% height=50%>

## Environment
+ Python 3.6.4
+ Scala 2.11
+ JDK 1.8 
+ Spark 2.4.4



