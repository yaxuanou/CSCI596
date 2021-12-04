import numpy as np
import xgboost as xgb
from itertools import combinations
import math
import random
from operator import itemgetter
from operator import add
from pyspark import SparkContext
import sys
import csv
from pyspark import SparkConf
import time
import json
from sklearn.model_selection import GridSearchCV


def process_user(x):
    return x[0][0], x[0][1], x[1][0], x[1][1]


def count_checkin(x):
    return sum(x.values())


def count_list(x):
    if x != "None":
        list_x = x.split(", ")
        return len(list_x)
    else:
        return 0


def count_compliments(x):
    sum_ = 0
    for x_ in x:
        sum_ += x_
    return sum_


def total_star(x, y):
    return x * y


if __name__ == "__main__":
    start_time = time.time()
    # folder_path = sys.argv[1]
    # test_file_name = sys.argv[2]
    # output_file_name = sys.argv[3]
    folder_path = './'
    test_file_name = 'yelp_val.csv'
    output_file_name = 'test.csv'
    # top N number

    # conf = SparkConf()
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("WARN")

    label = "u_id, b_id," \
            "user_useful, user_funny, user_cool, user_review_count, user_friend_count, user_fans, user_elite, user_average_stars, user_compliment," \
            " bus_average_stars, bus_review_count, chickInSum, star"

    all_train_rdd = sc.textFile(folder_path + 'yelp_train.csv')
    header_train = all_train_rdd.first()
    train_rdd = all_train_rdd.filter(lambda x: x != header_train).map(
        lambda x: (str(x.split(',')[0]), (str(x.split(',')[1]), float(x.split(',')[2]))))

    all_val_rdd = sc.textFile(test_file_name)
    header_val = all_val_rdd.first()
    val_rdd = all_val_rdd.filter(lambda x: x != header_train).map(
        lambda x: (str(x.split(',')[0]), str(x.split(',')[1])))

    # all_tip_rdd = sc.textFile(folder_path + 'tip.json').map(lambda line: json.loads(line))
    # tip_rdd = all_tip_rdd.map(lambda k: ((k['user_id'], k['business_id']), float(k['likes'])))
    #
    # all_review_rdd = sc.textFile(folder_path + 'review_train.json').map(lambda line: json.loads(line))
    # review_rdd = all_review_rdd.map(lambda k: ((k['user_id'], k['business_id']), float(k['useful'])))

    all_user_rdd = sc.textFile(folder_path + 'user.json').map(lambda line: json.loads(line))
    user_rdd = all_user_rdd.map(lambda k: (k['user_id'], (float(k['useful']), float(k['funny']), float(k['cool']),
                                                          float(k['review_count']), float(count_list(k['friends'])),
                                                          float(k['fans']), count_list(k['elite']),
                                                          float(k['average_stars']),
                                                          float(count_compliments(
                                                              [k['compliment_hot'], k['compliment_more'],
                                                               k['compliment_profile'], k['compliment_cute'],
                                                               k['compliment_list'], k['compliment_note'],
                                                               k['compliment_plain'], k['compliment_cool'],
                                                               k['compliment_funny'], k['compliment_writer'],
                                                               k['compliment_photos']])))))

    all_business_rdd = sc.textFile(folder_path + 'business.json').map(lambda line: json.loads(line))
    business_rdd = all_business_rdd.map(
        lambda k: (k['business_id'], (float(k['stars']), float(total_star(k['stars'], k['review_count'])))))

    all_checkin_rdd = sc.textFile(folder_path + 'checkin.json').map(lambda line: json.loads(line))
    checkin_rdd = all_checkin_rdd.map(lambda k: (k['business_id'], count_checkin(k['time'])))

    train_data_rdd = train_rdd.leftOuterJoin(user_rdd).map(
        lambda k: (k[1][0][0], (k[0], k[1][0][1], k[1][1]))).leftOuterJoin(business_rdd).leftOuterJoin(
        checkin_rdd).map(
        lambda k: ((k[1][0][0][0], k[0]), k[1][0][0][1], k[1][0][0][2], k[1][0][1], k[1][1])).map(
        lambda s: (s[0], (s[1],) + s[2] + s[3] + (s[4],)))
    # .leftOuterJoin(tip_rdd).take(1)
    # .leftOuterJoin(review_rdd).take(2)

    val_data_rdd = val_rdd.leftOuterJoin(user_rdd).map(lambda k: (k[1][0], (k[0], k[1][1]))).leftOuterJoin(
        business_rdd).leftOuterJoin(checkin_rdd).map(
        lambda k: ((k[1][0][0][0], k[0]), k[1][0][0][1], k[1][0][1], k[1][1])).map(
        lambda s: (s[0], s[1] + s[2] + (s[3],)))

    X_train = train_data_rdd.map(lambda x: x[1][1:]).collect()
    X_test = val_data_rdd.map(lambda x: x[1]).collect()
    y_train = train_data_rdd.map(lambda x: x[1][0]).collect()

    # xgb_model = xgb.XGBRegressor()
    # param_dict = {
    #     # 'num_boost_round': [800, 900, 1000, 1100, 1200, 1500],
    #     # 'eta': [0.05, 0.1, 0.3],
    #     # 'max_depth': [6, 9, 12],
    #     # 'subsample': [0.9, 1.0],
    #     # 'colsample_bytree': [0.9, 1.0],
    #     'min_child_weight': [4, 5],
    #     'gamma': [i / 10.0 for i in range(3, 6)],
    #     'subsample': [i / 10.0 for i in range(6, 11)],
    #     'colsample_bytree': [i / 10.0 for i in range(6, 11)],
    #     'max_depth': [8, 10, 12, 15]
    # }

    # clf = GridSearchCV(xgb_model, param_dict)
    # clf.fit(np.array(X_train), np.array(y_train))
    # print(clf.best_score_)
    # print(clf.best_params_)
    params = {
        'booster': 'gbtree',
        'objective': 'reg:linear',
        'gamma': 0.1,
        'max_depth': 10,
        'lambda': 10,
        'subsample': 0.8,
        'colsample_bytree': 0.8,
        'min_child_weight': 15,
        'eta': 0.01,
        'seed': 1000,
        'nthread': 4,
    }
    # 0.9812806561404572 'max_depth': 10,'lambda': 6,'min_child_weight': 6, # Vocareum 1100 0.981366575544018
    # 0.9809857755252342 10,10,1200
    # 0.980761196109323 12，12，12，1200
    # 0.9808774945784038 10,10,10,2000
    # 0.9799xxxxxxxxxxxx 10,10,10,2000
    """
    0.9799745113303628 10,10,10,2000, linear
    01: 41277
    12: 15282
    23: 5257
    34: 795
    4: 1
    """
    """
    0.9799039618107201 1159.6270339488983 10,10,12 2100, linear
    01: 41260
    12: 15271
    23: 5253
    34: 802
    4: 1
    """
    """
    0.9798051749435177
    01: 41242
    12: 15276
    23: 5265
    34: 797
    4: 1
    15，1500
    962.5620839595795
    """
    """
    0.9798165583180006
    01: 41181
    12: 15277
    23: 5278
    34: 786
    4: 1
    12，1500
    990.4941728115082
    """
    """
    0.9799654122058808 1300，12
    """
    xgtrain = xgb.DMatrix(np.array(X_train), np.array(y_train))
    num_rounds = 1800
    model = xgb.train(params, xgtrain, num_rounds)
    xgtest = xgb.DMatrix(np.array(X_test))
    ans = model.predict(xgtest)

    result = val_data_rdd.map(lambda x: x[0]).collect()
    for i in range(len(ans)):
        result[i] += (ans[i],)

    with open(output_file_name, "w+") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["user_id", " business_id", " prediction"])
        writer.writerows(result)
    print('Duration:', time.time() - start_time)
