from pyspark import SparkContext

import math

if __name__ == "__main__":
    # input_file_name = sys.argv[1]
    # output_file_name = sys.argv[2]
    # input_file_name = 'yelp_train.csv'
    # output_file_name = 'test.csv'
    file1 = 'test.csv'
    file2 = 'yelp_val.csv'

    sc = SparkContext.SparkConf()
    all_rdd1 = sc.textFile(file1)
    header1 = all_rdd1.first()
    clean_rdd1 = all_rdd1.filter(lambda x: x != header1).map(
        lambda x: (str(x.split(',')[0]), str(x.split(',')[1]), float(x.split(',')[2]))).map(
        lambda x: ((x[0], x[1]), x[2])).collectAsMap()
    all_rdd2 = sc.textFile(file2)
    header2 = all_rdd2.first()
    clean_rdd2 = all_rdd2.filter(lambda x: x != header2).map(
        lambda x: (str(x.split(',')[0]), str(x.split(',')[1]), float(x.split(',')[2]))).map(
        lambda x: ((x[0], x[1]), x[2])).collectAsMap()
    resultSum = 0
    list01 = 0
    list12 = 0
    list23 = 0
    list34 = 0
    list4 = 0
    for key in clean_rdd2:
        diff = clean_rdd1[key] - clean_rdd2[key]
        if 1 > diff >= 0:
            list01 += 1
        elif 2 > diff >= 1:
            list12 += 1
        elif 3 > diff >= 2:
            list23 += 1
        elif 4 > diff >= 3:
            list34 += 1
        elif diff >= 4:
            list4 += 1
        resultSum += diff ** 2
    print(math.sqrt(resultSum / len(clean_rdd1)))
    print('01:', list01)
    print('12:', list12)
    print('23:', list23)
    print('34:', list34)
    print('4:', list4)
