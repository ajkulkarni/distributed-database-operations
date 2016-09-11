#!/usr/bin/python2.7
#
# Implementation for the assignement
#

import psycopg2
import psycopg2.extras
import sys, os

DATABASE_NAME = 'dds_assgn1'
range_partitions = 0
round_partitions = 0
current_partition_index = 1

def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    return
    cur = openconnection.cursor()
    cur.execute("SELECT * FROM information_schema.tables WHERE table_name=%s", (ratingstablename,))

    if (bool(cur.rowcount) <> True):
        cur.execute("CREATE TABLE "+ratingstablename+" (UserId int, MovieId int, Rating decimal);")
        #cur.execute("CREATE TABLE test_"+ratingstablename+" (UserId int, MovieId int, Rating decimal);")
        print "Table created"
    else:
        print "Table already created"

    with open(ratingsfilepath) as f:
        count = 0
        last_line = None
        query_string = ()

        for line in f:
            if not last_line == None:
                table_data = last_line.splitlines()[0].split('::')
                print table_data[0]
                count += 1
                try:
                    query_string = (cur.mogrify('(%s,%s,%s)',(table_data[0], table_data[1], table_data[2])),) + query_string
                    if (count == 2000):
                        query_str = ','.join(query_string)
                        cur.execute("INSERT INTO "+ratingstablename+" VALUES " + query_str)
                        #cur.execute("INSERT INTO test_"+ratingstablename+" VALUES " + query_str)
                        count = 0
                        query_string = ()
                except Exception, e:
                    print e
            last_line = line

        if not last_line == None:
            table_data = last_line.splitlines()[0].split('::')
            query_string = (cur.mogrify('(%s,%s,%s)',(table_data[0], table_data[1], table_data[2])),) + query_string
            query_str = ','.join(query_string)
            cur.execute("INSERT INTO "+ratingstablename+" VALUES " + query_str)
            #cur.execute("INSERT INTO test_"+ratingstablename+" VALUES " + query_str)
    cur.execute("CREATE TABLE test_"+ratingstablename+" AS SELECT * FROM "+ratingstablename+";")


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    global range_partitions
    range_partitions = numberofpartitions
    return
    cur = openconnection.cursor()
    range_start = 0.0
    range_end = 5.0/numberofpartitions
    increment = 5.0/numberofpartitions

    cur.execute("TRUNCATE "+ratingstablename+";")

    for n in range(numberofpartitions):
        if(n == numberofpartitions - 1):
            range_end = 5.05
        try:
            create = "CREATE TABLE "+ratingstablename+str(n)+" (CHECK (rating >= "+str(range_start)+" AND rating < "+str(range_end)+")) INHERITS ("+ratingstablename+");"
            cur.execute(create)
            insert = "INSERT INTO "+ratingstablename+str(n)+" (userid, movieid, rating) SELECT userid, movieid, rating FROM test_"+ratingstablename+" WHERE rating >= "+str(range_start)+" AND rating < "+str(range_end)+";"
            cur.execute(insert)
        except Exception, e:
            print e

        range_start += increment
        range_end += increment
    #Clean Up
    #cur.execute("DROP TABLE test_"+ratingstablename+";")


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    global round_partitions
    round_partitions = numberofpartitions
    return
    cursor = openconnection.cursor()
    for n in range(numberofpartitions):
        try:
            create = "CREATE TABLE round_"+ratingstablename+str(n)+" (UserId int, MovieId int, Rating decimal) INHERITS ("+ratingstablename+");"
            cursor.execute(create)
        except Exception, e:
            print e

    cur = openconnection.cursor('cursor_space_x', cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT * FROM "+ratingstablename+";")

    current_partition = 0
    count = 0
    query_string = ""

    for row in cur:
        count += 1
        try:
            build_query = "INSERT INTO round_"+ratingstablename+str(current_partition)+" (userid, movieid, rating) VALUES ("+str(row[0])+","+str(row[1])+","+str(row[2])+"); "
            query_string = build_query + query_string
            current_partition += 1;
            if(current_partition == numberofpartitions):
                current_partition = 0;
            if (count == 2000):
                cursor.execute(query_string)
                count = 0
                query_string = ""
        except Exception, e:
            print e
    if(query_string <> ""):
        cursor.execute(query_string)
    global current_partition_index
    current_partition_index = current_partition
        #cursor.execute("INSERT INTO round_"+ratingstablename+str(current_partition)+" (userid, movieid, rating) VALUES (%s, %s, %s)", (row[0], row[1], row[2]))
        #current_partition += 1;
        #if(current_partition == numberofpartitions):
            #current_partition = 0;



def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    global current_partition_index
    try:
        cur.execute("INSERT INTO round_"+ratingstablename+str(current_partition_index)+" (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
        current_partition_index += 1
        if(current_partition_index == round_partitions):
            current_partition = 0;
    except Exception, e:
        print e


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    return
    cur = openconnection.cursor()
    range_start = 0.0
    range_end = 5.0/range_partitions
    increment = 5.0/range_partitions
    for n in range(range_partitions):
        if(n == range_partitions - 1):
            range_end = 5.05

        if(rating >= range_start and rating < range_end):

            try:
                cur.execute("INSERT INTO "+ratingstablename+str(n)+" (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
            except Exception, e:
                print e

        range_start += increment
        range_end += increment

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    rangepartition('test', 5, openconnection)
    rangeinsert('test', 71568, 122, 2.5, openconnection)
    roundrobininsert('test', 71568, 122, 2.5, openconnection)
    roundrobinpartition('test', 7, openconnection)


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings('test','ratings.dat', con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
