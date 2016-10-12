#!/usr/bin/python2.7
#
# Author: Ajay Kulkarni
#

import psycopg2
import os
import sys

def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #RangeQuery on Range Partitiom
    target = open('RangeQueryOut.txt', 'w+')
    if(ratingMinValue > ratingMaxValue):
        target.close()
        return
    if(ratingMinValue < 0):
        ratingMinValue = 0
    if(ratingMaxValue > 5):
        ratingMaxValue = 5
    cursor = openconnection.cursor()
    cur = openconnection.cursor()
    cursor.execute("SELECT MaxRating FROM RangeRatingsMetadata ORDER BY MaxRating ASC LIMIT 1")
    max_rating = -1
    for row in cursor:
        max_rating = row[0]
    comparison_operator1 = ">"
    comparison_operator2 = "<"
    if(ratingMaxValue < max_rating):
        comparison_operator1 = ">="
        comparison_operator2 = "<="
    sql_str = "SELECT PartitionNum FROM RangeRatingsMetadata WHERE (MinRating "+comparison_operator1+" "+str(ratingMinValue)+" OR MinRating "+comparison_operator2+" "+str(ratingMaxValue)+") OR (MaxRating BETWEEN "+str(ratingMinValue)+" AND "+str(ratingMaxValue)+")"
    cursor.execute(sql_str)
    partition_number = -1
    for row in cursor:
        partition_number = row[0]
        if(partition_number == -1):
            print "Something went wrong in fetching partition number"
            target.close()
            return
        cur.execute("SELECT * FROM RangeRatingsPart%d WHERE Rating BETWEEN %f AND %f" %(partition_number,ratingMinValue,ratingMaxValue))
        for row in cur:
            target.write("RangeRatingsPart"+str(partition_number)+","+str(row[0])+","+str(row[1])+","+str(row[2]))
            target.write("\n")
    #RangeQuery on Round Robin Partition
    cursor.execute("SELECT PartitionNum FROM RoundRobinRatingsMetadata ORDER BY PartitionNum DESC LIMIT 1")
    number_of_partitions = -1
    for row in cursor:
        number_of_partitions = row[0]
    if(number_of_partitions == -1):
        print "Something went wrong in fetching number of partitions"
        target.close()
        return
    for i in range(number_of_partitions):
        cur.execute("SELECT * FROM RoundRobinRatingsPart%d WHERE Rating BETWEEN %f AND %f" %(i,ratingMinValue,ratingMaxValue))
        for row in cur:
            target.write("RoundRobinRatingsPart"+str(i)+","+str(row[0])+","+str(row[1])+","+str(row[2]))
            target.write("\n")
    target.close()


def PointQuery(ratingsTableName, ratingValue, openconnection):
    #PointQuery on Range Partition
    target = open('PointQueryOut.txt', 'w+')
    if (ratingValue < 0 or ratingValue > 5):
        target.close()
        return
    cursor = openconnection.cursor()
    cur = openconnection.cursor()
    cursor.execute("SELECT MaxRating FROM RangeRatingsMetadata ORDER BY MaxRating ASC LIMIT 1")
    max_rating = -1
    for row in cursor:
        max_rating = row[0]
    comparison_operator = "<"
    if(ratingValue <= max_rating):
        comparison_operator = "<="
    cursor.execute("SELECT PartitionNum FROM RangeRatingsMetadata WHERE MinRating "+comparison_operator+"%f AND MaxRating >=%f" %(ratingValue,ratingValue))
    partition_number = -1
    for row in cursor:
        partition_number = row[0]
    if(partition_number == -1):
        print "Something went wrong in fetching partition number"
        target.close()
        return
    cursor.execute("SELECT * FROM RangeRatingsPart%d WHERE Rating = %f" %(partition_number,ratingValue))
    for row in cursor:
        target.write("RangeRatingsPart"+str(partition_number)+","+str(row[0])+","+str(row[1])+","+str(row[2]))
        target.write("\n")
    #PointQuery on Round Robin Partition
    cursor.execute("SELECT PartitionNum FROM RoundRobinRatingsMetadata ORDER BY PartitionNum DESC LIMIT 1")
    number_of_partitions = -1
    for row in cursor:
        number_of_partitions = row[0]
    if(number_of_partitions == -1):
        print "Something went wrong in fetching number of partitions"
        target.close()
        return
    for i in range(number_of_partitions):
        cur.execute("SELECT * FROM RoundRobinRatingsPart%d WHERE Rating = %f" %(i,ratingValue))
        for row in cur:
            target.write("RoundRobinRatingsPart"+str(i)+","+str(row[0])+","+str(row[1])+","+str(row[2]))
            target.write("\n")
    target.close()
