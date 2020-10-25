import pyorient.orient as pyorient

db_name = "test_db"

# Connect to db
client = pyorient.OrientDB("localhost", 2424)
session_id = client.connect("root", "root")
print("Retrieved session id: " + str(session_id))

# Create a db
# client.db_create(db_name, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_PLOCAL)

# Drop db
# client.db_drop(db_name)

# Open the db
# client.db_open(db_name, "root", "root")

