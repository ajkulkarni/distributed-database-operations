#!/usr/bin/python2.7
#
# Implementation for the assignement
# Ajay Kulkarni
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'
range_partitions = 0
round_partitions = 0
current_partition_index = 1

def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT * FROM information_schema.tables WHERE table_name=%s", (ratingstablename,))

    if (bool(cur.rowcount) == True):
        cur.execute("DROP TABLE "+ratingstablename+" CASCADE;")

    cur.execute("CREATE TABLE "+ratingstablename+" (UserId int, drop1 varchar, MovieId int, drop2 varchar, Rating decimal, drop3 varchar, drop4 varchar);")

    with open(ratingsfilepath) as f:
        cur.copy_from(f, ratingstablename, sep=":")

    cur.execute("ALTER TABLE "+ratingstablename+" DROP COLUMN drop1, DROP COLUMN drop2, DROP COLUMN drop3, DROP COLUMN drop4;")
    openconnection.commit()
    cur.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    deletepartitionsandexit(openconnection)

    if (numberofpartitions < 0 or isinstance(numberofpartitions, float)):
        return

    global range_partitions
    range_partitions = numberofpartitions
    cur = openconnection.cursor()
    range_start = 0.0
    range_end = 5.0/numberofpartitions
    increment = 5.0/numberofpartitions

    for n in range(numberofpartitions):
        if(n == numberofpartitions - 1):
            range_end = 5.0
        comp = ""
        try:
            if(n == 0):
                comp = ">="
            else:
                comp = ">"
            create = "CREATE TABLE range_part"+str(n)+" (CHECK (rating "+comp+" "+str(range_start)+" AND rating <= "+str(range_end)+")) INHERITS ("+ratingstablename+");"
            cur.execute(create)
            insert = "INSERT INTO range_part"+str(n)+" (userid, movieid, rating) SELECT userid, movieid, rating FROM "+ratingstablename+" WHERE rating "+comp+" "+str(range_start)+" AND rating <= "+str(range_end)+";"
            cur.execute(insert)
        except Exception, e:
            print e

        range_start += increment
        range_end += increment
    openconnection.commit()
    cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    deletepartitionsandexit(openconnection)

    if (numberofpartitions < 0 or isinstance(numberofpartitions, float)):
        return

    global round_partitions
    round_partitions = numberofpartitions
    cursor = openconnection.cursor()

    for n in range(numberofpartitions):
        try:
            create = "CREATE TABLE rrobin_part"+str(n)+" (UserId int, MovieId int, Rating decimal) INHERITS ("+ratingstablename+");"
            cursor.execute(create)
        except Exception, e:
            print e

    try:
        cur = openconnection.cursor()
        cur.execute("SELECT * FROM "+ratingstablename+";")
    except Exception, e:
        print e

    current_partition = 0
    count = 0
    query_string = ""

    for row in cur:
        count += 1
        try:
            build_query = "INSERT INTO rrobin_part"+str(current_partition)+" (userid, movieid, rating) VALUES ("+str(row[0])+","+str(row[1])+","+str(row[2])+"); "
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

    if len(query_string) > 0:
        cursor.execute(query_string)

    global current_partition_index
    current_partition_index = current_partition
    openconnection.commit()
    cur.close()
    cursor.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    if (rating < 0 or rating > 5 or round_partitions == 0):
        return

    cur = openconnection.cursor()
    global current_partition_index

    try:
        cur.execute("INSERT INTO rrobin_part"+str(current_partition_index)+" (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
        current_partition_index += 1
        if(current_partition_index == round_partitions):
            current_partition = 0;
    except Exception, e:
        print e
    openconnection.commit()
    cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    if (rating < 0 or rating > 5 or range_partitions == 0):
        return

    cur = openconnection.cursor()
    range_start = 0.0
    range_end = 5.0/range_partitions
    increment = 5.0/range_partitions

    for n in range(range_partitions):
        if(n == range_partitions - 1):
            range_end = 5.0

        if((rating == 0 or rating > range_start) and rating <= range_end):
            try:
                cur.execute("INSERT INTO range_part"+str(n)+" (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
            except Exception, e:
                print e

        range_start += increment
        range_end += increment
    openconnection.commit()
    cur.close()

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

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    for n in range(range_partitions):
        cur.execute("DROP TABLE IF EXISTS range_part"+str(n))
    for n in range(round_partitions):
        cur.execute("DROP TABLE IF EXISTS rrobin_part"+str(n))
    openconnection.commit()
    cur.close()

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
    pass
    #rangepartition('Ratings', 5, openconnection)
    #rangeinsert('Ratings', 71568, 122, 2.5, openconnection)
    #roundrobinpartition('Ratings', 7, openconnection)
    #roundrobininsert('Ratings', 71568, 122, 2.5, openconnection)
    #deletepartitionsandexit(openconnection)


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
            loadratings('Ratings','ratings.dat', con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
