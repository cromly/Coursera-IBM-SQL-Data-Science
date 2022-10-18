# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import ibm_db
import pandas
import ibm_db_dbi

#Replace the placeholder values with your actual Db2 hostname, username, and password:
dsn_hostname = "824dfd4d-99de-440d-9991-629c01b3832d.bs2io90l08kqb1od8lcg.databases.appdomain.cloud"
dsn_uid = "nhs80496"        # e.g. "abc12345"
dsn_pwd = "bSHzPbvxr0UwXLQZ"      # e.g. "7dBZ3wWt9XN6$o0J"

dsn_driver = "{IBM DB2 ODBC DRIVER}"
dsn_database = "BLUDB"            # e.g. "BLUDB"
dsn_port = "30119"                # e.g. "32733"
dsn_protocol = "TCPIP"            # i.e. "TCPIP"
dsn_security = "SSL"              #i.e. "SSL"

#DO NOT MODIFY THIS CELL. Just RUN it with Shift + Enter
#Create the dsn connection string
dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd,dsn_security)

#print the connection string to check correct values are specified
print(dsn)

#DO NOT MODIFY THIS CELL. Just RUN it with Shift + Enter
#Create database connection

#Connecting with IBM_DB
try:
    #connection for regular Python
    conn = ibm_db.connect(dsn, "", "")
    print("Connected to database ibm_db: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname, "\n")

except:
    print ("Unable to connect ibm_db: ", ibm_db.conn_errormsg() )


#Connecting with IBM_DB_DBI
try:
    pconn = ibm_db_dbi.Connection(conn)
    print("Connected to database ibm_db_dbi: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname, "\n")

except:
    print ("Unable to connect ibm_db: ")

#LOUIS> Create a cursor
my_cursor = pconn.cursor()

#Retrieve Metadata for the Database Server
server = ibm_db.server_info(conn)

print ("DBMS_NAME: ", server.DBMS_NAME)
print ("DBMS_VER:  ", server.DBMS_VER)
print ("DB_NAME:   ", server.DB_NAME)

# Retrieve Metadata for the Database Client / Driver
client = ibm_db.client_info(conn)

print ("DRIVER_NAME:          ", client.DRIVER_NAME)
print ("DRIVER_VER:           ", client.DRIVER_VER)
print ("DATA_SOURCE_NAME:     ", client.DATA_SOURCE_NAME)
print ("DRIVER_ODBC_VER:      ", client.DRIVER_ODBC_VER)
print ("ODBC_VER:             ", client.ODBC_VER)
print ("ODBC_SQL_CONFORMANCE: ", client.ODBC_SQL_CONFORMANCE)
print ("APPL_CODEPAGE:        ", client.APPL_CODEPAGE)
print ("CONN_CODEPAGE:        ", client.CONN_CODEPAGE)

#Lets first drop the table INSTRUCTOR in case it exists from a previous attempt

try:
    dropQuery = "drop table INSTRUCTOR"
    dropStmt = ibm_db.exec_immediate(conn, dropQuery)

except:
    print("Table instructor does not exist \n")

#Create a new table "instructor"
createQuery = "CREATE TABLE instructor(id INTEGER PRIMARY KEY NOT NULL, " \
              "fname VARCHAR(20), " \
              "lname VARCHAR(20), " \
              "city VARCHAR(20), " \
              "code CHAR(2))"

createStmt = ibm_db.exec_immediate(conn, createQuery)


insertQuery = "INSERT INTO instructor VALUES (1, 'Rav', 'Ahuja', 'TORONTO', 'CA')"
insertStmt = ibm_db.exec_immediate(conn, insertQuery)

insertQuery2 = "INSERT INTO instructor VALUES (2, 'Raul', 'Chong', 'Markham', 'CA')," \
               " (3, 'Hima', 'Vasudevan', 'Chicago', 'US')"
insertStmt2 = ibm_db.exec_immediate(conn, insertQuery2)

#Construct the query that retrieves all rows from the INSTRUCTOR table
selectQuery = "SELECT * FROM instructor"

#Execute the statement
selectStmt = ibm_db.exec_immediate(conn, selectQuery)

#Fetch the Dictionary (for the first row only)
ibm_db.fetch_both(selectStmt)

#Fetch the rest of the rows and print the ID and FNAME for those rows
while ibm_db.fetch_row(selectStmt) != False:
    print (" ID:",  ibm_db.result(selectStmt, 0), " FNAME:",  ibm_db.result(selectStmt, "fname"))

#Update the table "indstructor"
updateQuery = "UPDATE instructor SET city='Moosetown' WHERE fname='Rav'"
updateStmt = ibm_db.exec_immediate(conn, updateQuery)


#In this step we will retrieve the contents of the INSTRUCTOR table into a Pandas dataframe
#query statement to retrieve all rows in INSTRUCTOR table
#retrieve the query results into a pandas dataframe
#print just the LNAME for first row in the pandas data frame
selectQuery = "SELECT * FROM instructor"

try:
    my_cursor.execute(selectQuery)
except Exception as err:
    print("Error on Execute", err)

try:
    df = pandas.DataFrame(my_cursor.fetchall())
    df.columns = [col[0] for col in my_cursor.description]
except Exception as err:
    print("Error on Fetch", err)

print(df)

# pdf = pandas.read_sql(selectQuery, pconn)
# pdf.lname[0]

#print the entire data frame
# pdf

ibm_db.close(conn)


