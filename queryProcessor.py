#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #RangeQuery in Range
    if (ratingMinValue < 0 or ratingMaxValue > 5):
        return
    cursor = openconnection.cursor()
    cur = openconnection.cursor()
    cursor.execute("SELECT PartitionNum FROM RangeRatingsMetadata WHERE (MinRating BETWEEN %f AND %f) OR (MaxRating BETWEEN %f AND %f)" %(ratingMinValue,ratingMaxValue,ratingMinValue,ratingMaxValue))
    partition_number = -1
    for row in cursor:
        partition_number = row[0]
        if(partition_number == -1):
            print "Something went wrong in fetching partition number"
            return
        cur.execute("SELECT * FROM RangeRatingsPart%d WHERE Rating BETWEEN %f AND %f" %(partition_number,ratingMinValue,ratingMaxValue))
        for row in cur:
            print "RangeRatingsPart"+str(partition_number)+","+str(row[0])+","+str(row[1])+","+str(row[2])

    #RangeQuery in roundRobinPartition
    cursor.execute("SELECT PartitionNum FROM RoundRobinRatingsMetadata ORDER BY PartitionNum DESC LIMIT 1")
    number_of_partitions = -1
    for row in cursor:
        number_of_partitions = row[0]
    if(number_of_partitions == -1):
        print "Something went wrong in fetching number of partitions"
        return
    for i in range(number_of_partitions):
        cur.execute("SELECT * FROM RoundRobinRatingsPart%d WHERE Rating BETWEEN %f AND %f" %(i,ratingMinValue,ratingMaxValue))
        for row in cur:
            print "RoundRobinRatingsPart"+str(i)+","+str(row[0])+","+str(row[1])+","+str(row[2])


def PointQuery(ratingsTableName, ratingValue, openconnection):
    #PointQuery in Range
    if (ratingValue < 0 or ratingValue > 5):
        return
    cursor = openconnection.cursor()
    cur = openconnection.cursor()
    cursor.execute("SELECT PartitionNum FROM RangeRatingsMetadata WHERE MinRating <=%f AND MaxRating >=%f" %(ratingValue,ratingValue))
    partition_number = -1
    for row in cursor:
        partition_number = row[0]
    if(partition_number == -1):
        print "Something went wrong in fetching partition number"
        return
    cursor.execute("SELECT * FROM RangeRatingsPart%d WHERE Rating = %f" %(partition_number,ratingValue))
    for row in cursor:
        print "RangeRatingsPart"+str(partition_number)+","+str(row[0])+","+str(row[1])+","+str(row[2])

    #PointQuery in roundRobinPartition
    cursor.execute("SELECT PartitionNum FROM RoundRobinRatingsMetadata ORDER BY PartitionNum DESC LIMIT 1")
    number_of_partitions = -1
    for row in cursor:
        number_of_partitions = row[0]
    if(number_of_partitions == -1):
        print "Something went wrong in fetching number of partitions"
        return
    for i in range(number_of_partitions):
        cur.execute("SELECT * FROM RoundRobinRatingsPart%d WHERE Rating = %f" %(i,ratingValue))
        for row in cur:
            print "RoundRobinRatingsPart"+str(i)+","+str(row[0])+","+str(row[1])+","+str(row[2])
