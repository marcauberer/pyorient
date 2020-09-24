#  Copyright 2020 Niko Usai <usai.niko@gmail.com>, http://mogui.it; Marc Auberer, https://marc-auberer.de
#
#  this file is part of pyorient
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#   limitations under the License.

__author__ = 'mogui <mogui83@gmail.com>, Marc Auberer <marc.auberer@sap.com>'

# Driver constants
NAME = "OrientDB Python binary client (pyorient)"
VERSION = "2.0.0"
SUPPORTED_PROTOCOL = 38
ERROR_ON_NEWER_PROTOCOL = True

# Type constants
BOOLEAN = 1
BYTE = 2
SHORT = 3
INT = 4
LONG = 5
BYTES = 6  # Used for binary data
STRING = 7
RECORD = 8
STRINGS = 9
CHAR = 10
LINK = 11

# Type map
TYPE_MAP = {
    'BOOLEAN': 0,
    'INTEGER': 1,
    'SHORT': 2,
    'LONG': 3,
    'FLOAT': 4,
    'DOUBLE': 5,
    'DATETIME': 6,
    'STRING': 7,
    'BINARY': 8,
    'EMBEDDED': 9,
    'EMBEDDEDLIST': 10,
    'EMBEDDEDSET': 11,
    'EMBEDDEDMAP': 12,
    'LINK': 13,
    'LINKLIST': 14,
    'LINKSET': 15,
    'LINKMAP': 16,
    'BYTE': 17,
    'TRANSIENT': 18,
    'DATE': 19,
    'CUSTOM': 20,
    'DECIMAL': 21,
    'LINKBAG': 22,
    'ANY': 23
}

# Field types, needed for decoding
# we have the type definition and the number of first bytes to read
FIELD_BOOLEAN = {"type": BOOLEAN, "bytes": 1, "struct": None}
FIELD_BYTE = {"type": BYTE, "bytes": 1, "struct": None}
FIELD_CHAR = {"type": CHAR, "bytes": 1, "struct": None}
FIELD_SHORT = {"type": SHORT, "bytes": 2, "struct": None}
FIELD_INT = {"type": INT, "bytes": 4, "struct": None}
FIELD_LONG = {"type": LONG, "bytes": 8, "struct": None}
FIELD_BYTES = {"type": BYTES, "bytes": 4, "struct": None}
FIELD_STRING = {"type": STRING, "bytes": 4, "struct": None}
FIELD_STRINGS = {"type": STRINGS, "bytes": 4, "struct": None}
FIELD_RECORD = {"type": RECORD, "bytes": None, "struct": [
    FIELD_CHAR,  # record_type
    FIELD_SHORT,  # record_clusterID
    FIELD_LONG,  # record_position
    FIELD_INT,  # record_version
    FIELD_BYTES  # record_content
]}
FIELD_TYPE_LINK = {"type": LINK, "bytes": None, "struct": [
    FIELD_SHORT,  # record_clusterID
    FIELD_LONG,  # record_position
]}

# Message constants
SHUTDOWN = "ShutdownMessage"
CONNECT = "ConnectMessage"
DB_OPEN = "DbOpenMessage"
DB_CREATE = "DbCreateMessage"
DB_CLOSE = "DbCloseMessage"
DB_EXIST = "DbExistsMessage"
DB_DROP = "DbDropMessage"
DB_SIZE = "DbSizeMessage"
DB_COUNT_RECORDS = "DbCountRecordsMessage"
DATA_CLUSTER_ADD = "DataClusterAddMessage"
DATA_CLUSTER_DROP = "DataClusterDropMessage"
DATA_CLUSTER_COUNT = "DataClusterCountMessage"
DATA_CLUSTER_DATA_RANGE = "DataClusterDataRangeMessage"
RECORD_LOAD = "RecordLoadMessage"
RECORD_CREATE = "RecordCreateMessage"
RECORD_UPDATE = "RecordUpdateMessage"
RECORD_DELETE = "RecordDeleteMessage"
COMMAND = "CommandMessage"
DB_RELOAD = "DbReloadMessage"
TX_COMMIT = "TxCommitMessage"

# Orient Operations
SHUTDOWN_OP = chr(1)
CONNECT_OP = chr(2)
DB_OPEN_OP = chr(3)
DB_CREATE_OP = chr(4)
DB_CLOSE_OP = chr(5)
DB_EXIST_OP = chr(6)
DB_DROP_OP = chr(7)
DB_SIZE_OP = chr(8)
DB_COUNT_RECORDS_OP = chr(9)
DATA_CLUSTER_ADD_OP = chr(10)
DATA_CLUSTER_DROP_OP = chr(11)
DATA_CLUSTER_COUNT_OP = chr(12)
DATA_CLUSTER_DATA_RANGE_OP = chr(13)
HANDSHAKE_OP = chr(20)
RECORD_LOAD_OP = chr(30)
RECORD_CREATE_OP = chr(31)
RECORD_UPDATE_OP = chr(32)
RECORD_DELETE_OP = chr(33)
COMMAND_OP = chr(41)
TX_COMMIT_OP = chr(60)
DB_RELOAD_OP = chr(73)
DB_LIST_OP = chr(74)

# Database types
DB_TYPE_DOCUMENT = 'document'
DB_TYPE_GRAPH = 'graph'
DB_TYPES = (DB_TYPE_DOCUMENT, DB_TYPE_GRAPH)

# Storage types
STORAGE_TYPE_PLOCAL = 'plocal'
STORAGE_TYPE_MEMORY = 'memory'
STORAGE_TYPES = (STORAGE_TYPE_PLOCAL, STORAGE_TYPE_MEMORY)

# Query types
QUERY_SYNC = "com.orientechnologies.orient.core.sql.query.OSQLSynchQuery"
QUERY_ASYNC = "com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery"
QUERY_CMD = "com.orientechnologies.orient.core.sql.OCommandSQL"
QUERY_GREMLIN = "com.orientechnologies.orient.graph.gremlin.OCommandGremlin"
QUERY_SCRIPT = "com.orientechnologies.orient.core.command.script.OCommandScript"
QUERY_TYPES = (QUERY_SYNC, QUERY_ASYNC, QUERY_CMD, QUERY_GREMLIN, QUERY_SCRIPT)

# Record types
RECORD_TYPE_BYTES = 'b'
RECORD_TYPE_DOCUMENT = 'd'
RECORD_TYPE_FLAT = 'f'
RECORD_TYPES = (RECORD_TYPE_BYTES, RECORD_TYPE_DOCUMENT, RECORD_TYPE_FLAT)

# Cluster types
CLUSTER_TYPE_PHYSICAL = 'PHYSICAL'
CLUSTER_TYPE_MEMORY = 'MEMORY'
CLUSTER_TYPES = (CLUSTER_TYPE_PHYSICAL, CLUSTER_TYPE_MEMORY)

# Message type dictionary
MESSAGES = dict(
    # Server
    ConnectMessage="pyorient.messages.connection",
    ShutdownMessage="pyorient.messages.connection",

    DbOpenMessage="pyorient.messages.database",
    DbCloseMessage="pyorient.messages.database",
    DbExistsMessage="pyorient.messages.database",
    DbCreateMessage="pyorient.messages.database",
    DbDropMessage="pyorient.messages.database",
    DbCountRecordsMessage="pyorient.messages.database",
    DbReloadMessage="pyorient.messages.database",
    DbSizeMessage="pyorient.messages.database",
    DbListMessage="pyorient.messages.database",

    # Cluster
    DataClusterAddMessage="pyorient.messages.cluster",
    DataClusterCountMessage="pyorient.messages.cluster",
    DataClusterDataRangeMessage="pyorient.messages.cluster",
    DataClusterDropMessage="pyorient.messages.cluster",

    RecordCreateMessage="pyorient.messages.records",
    RecordDeleteMessage="pyorient.messages.records",
    RecordLoadMessage="pyorient.messages.records",
    RecordUpdateMessage="pyorient.messages.records",

    CommandMessage="pyorient.messages.commands",
    TxCommitMessage="pyorient.messages.commands",
)

# OTHER CONFIGURATIONS
SOCK_CONN_TIMEOUT = 30  # Socket timeout in seconds
