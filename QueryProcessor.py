#!/usr/bin/python2.7
#
# Author: Ajay Kulkarni
#

import psycopg2
import os
import sys

def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #RangeQuery on Range Partitiom
    if (ratingMinValue < 0 or ratingMaxValue > 5):
        return
    cursor = openconnection.cursor()
    cur = openconnection.cursor()
    cursor.execute("SELECT PartitionNum FROM RangeRatingsMetadata WHERE (MinRating BETWEEN %f AND %f) OR (MaxRating BETWEEN %f AND %f)" %(ratingMinValue,ratingMaxValue,ratingMinValue,ratingMaxValue))
    partition_number = -1
    target = open('RangeQueryOut.txt', 'w+')
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
    if (ratingValue < 0 or ratingValue > 5):
        return
    cursor = openconnection.cursor()
    cur = openconnection.cursor()
    cursor.execute("SELECT PartitionNum FROM RangeRatingsMetadata WHERE MinRating <=%f AND MaxRating >=%f" %(ratingValue,ratingValue))
    partition_number = -1
    for row in cursor:
        partition_number = row[0]
    target = open('PointQueryOut.txt', 'w+')
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
