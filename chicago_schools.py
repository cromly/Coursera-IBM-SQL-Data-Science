
import ibm_db
import ibm_db_dbi
import pandas

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

print(dsn)
conn = ibm_db.connect(dsn, "", "")
pconn = ibm_db_dbi.Connection(conn)
my_cursor = pconn.cursor()
selectQuery = "SELECT * FROM CHIGAGO LIMIT 20"

my_cursor.execute(selectQuery)
df = pandas.DataFrame(my_cursor.fetchall())
df.columns = [col[0] for col in my_cursor.description]



print(df)

ibm_db.close(conn)


