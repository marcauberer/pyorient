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

# Python imports
import socket
import struct
import select

# Local imports
from .serializations import OrientSerialization
from .utils import dlog
from .constants import SOCK_CONN_TIMEOUT, FIELD_SHORT, SUPPORTED_PROTOCOL, ERROR_ON_NEWER_PROTOCOL, MESSAGES,\
    DB_TYPE_DOCUMENT, STORAGE_TYPE_PLOCAL, TYPE_MAP, QUERY_GREMLIN, QUERY_CMD, QUERY_SCRIPT, QUERY_SYNC, QUERY_ASYNC
from .exceptions import PyOrientConnectionPoolException, PyOrientWrongProtocolVersionException,\
    PyOrientConnectionException, PyOrientBadMethodCallException


class OrientSocket(object):
    """
    Class representing the binary connection to the database, it does all the low level communication and holds information on server version and cluster map
    .. DANGER:: Should not be used directly
    :param host: hostname of the server to connect
    :param port: integer port of the server
    """
    def __init__(self, host, port, serialization_type=OrientSerialization.CSV):
        # Initialize attributes with default values
        self.connected = False
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.protocol = -1
        self.session_id = -1
        self.auth_token = b''
        self.db_opened = None
        self.serialization_type = serialization_type
        self.in_transaction = False
        self.props = None

    def get_connection(self):
        # Establish the socket connection and return the connected socket
        if not self.connected:
            self.connect()
        return self.socket

    def connect(self):
        """
        Connects to the database server
        could raise :class:`PyOrientConnectionPoolException`
        """
        dlog("Trying to connect ...")
        try:
            # Set timeout and connect socket to the provided host and port
            self.socket.settimeout(SOCK_CONN_TIMEOUT)
            self.socket.connect((self.host, self.port))

            # Read short value from server to check, if the server is working correctly
            _answer = self.socket.recv(FIELD_SHORT['bytes'])
            if len(_answer) != 2:  # A short is 2 bytes long
                # Close the socket and throw exception if the server is not working correctly
                self.socket.close()
                raise PyOrientConnectionPoolException("Server sent empty string", [])

            # Unpack protocol version
            self.protocol = struct.unpack('!h', _answer)[0]

            # Raise exception on higher protocol version than supported, if enabled
            if self.protocol > SUPPORTED_PROTOCOL and ERROR_ON_NEWER_PROTOCOL:
                raise PyOrientWrongProtocolVersionException("Protocol version " + str(self.protocol) + " is not "
                      "supported by this client version. Please check, if there's a new pyorient version available", [])

            self.connected = True
        except socket.error as e:
            # Catch the exception and raise it up as a PyOrientConnectionException
            self.connected = False
            raise PyOrientConnectionException("Socket error: %s" % e, [])

    def close(self):
        """
        Close the connection to the database server
        """
        # Stop connection
        self.socket.close()
        self.connected = False
        # Reset all attributes to default
        self.host = ''
        self.port = 0
        self.protocol = -1
        self.session_id = -1

    def write(self, buff):
        # This is a trick to detect server disconnection
        # or broken line issues because of
        """:see: https://docs.python.org/2/howto/sockets.html#when-sockets-die """

        try:
            _, ready_to_write, in_error = select.select([], [self.socket], [self.socket], 1)
        except select.error as e:
            self.connected = False
            self.socket.close()
            raise e

        if not in_error and ready_to_write:
            # Socket works -> send all data
            self.socket.sendall(buff)
            return len(buff)
        else:
            # Socket does not work -> close and raise exception
            self.connected = False
            self.socket.close()
            raise PyOrientConnectionException("Socket error", [])

    def read(self, _len_to_read):
        while True:
            # This is a trick to detect server disconnection
            # or broken line issues because of
            """:see: https://docs.python.org/2/howto/sockets.html#when-sockets-die """
            try:
                ready_to_read, _, in_error = select.select([self.socket, ], [], [self.socket, ], 30)
            except select.error as e:
                self.connected = False
                self.socket.close()
                raise e

            if len(ready_to_read) > 0:
                buf = bytearray(_len_to_read)
                view = memoryview(buf)
                while _len_to_read:
                    n_bytes = self.socket.recv_into(view, _len_to_read)
                    # Nothing read -> Server went down
                    if not n_bytes:
                        self.socket.close()
                        # TODO: Implement re-connection to another listener
                        raise PyOrientConnectionException("Server seems to went down", [])

                    # Shorten view and _len_to_read by n_bytes
                    view = view[n_bytes:]
                    _len_to_read -= n_bytes
                # Read successfully, return result
                return bytes(buf)

            # Close connection, if error(s) occurred
            if len(in_error) > 0:
                self.socket.close()
                raise PyOrientConnectionException("Socket error", [])


class OrientDB(object):
    """
    OrientDB client object

    Point of entrance to use the basic commands you can issue to the server
    :param host: hostname of the server to connect  defaults to localhost
    :param port: integer port of the server         defaults to 2424
    """
    _connection = None
    _auth_token = None

    def __init__(self, host='localhost', port=2424, serialization_type=OrientSerialization.CSV):
        if not isinstance(host, OrientDB):
            connection = OrientSocket(host, port, serialization_type)
        else:
            connection = host

        #: an :class:`OrientVersion <OrientVersion>` object representing connected server version, None if
        #: not connected
        self.version = None

        #: array of :class:`OrientCluster <OrientCluster>` representing the connected database clusters
        self.clusters = []

        #: array of :class:`OrientNode <OrientNode>` if the connected server is in a distributed cluster config
        self.nodes = []

        self._cluster_map = None
        self._cluster_reverse_map = None
        self._connection = connection
        self._serialization_type = serialization_type

    def close(self):
        self._connection.close()

    def __getattr__(self, item):
        # No special handling for private attributes / methods
        if item.startswith("_"):
            return super(OrientDB, self).__getattr__(item)

        # Find class from dictionary by constructing the key
        _names = "".join([i.capitalize() for i in item.split('_')])  # Snake Case to Camel Case converter
        _Message = self.get_message(_names + "Message")

        # Generate a wrapper function and return it
        def wrapper(*args, **kw):
            return _Message.prepare(args).send().fetch_response()
        return wrapper

    def _reload_clusters(self):
        # Re-generate dictionaries from clusters
        self._cluster_map = dict([(cluster.name, cluster.id) for cluster in self.clusters])
        self._cluster_reverse_map = dict([(cluster.id, cluster.name) for cluster in self.clusters])

    def get_class_position(self, cluster_name):
        """
        Get the cluster position (id) by the name of the cluster

        :param cluster_name: cluster name
        :return: int cluster id
        """
        return self._cluster_map[cluster_name.lower()]

    def get_class_name(self, position):
        """
        Get the cluster name by the position (id) of the cluster

        :param position: cluster id
        :return: string cluster name
        """
        return self._cluster_reverse_map[position]

    def set_session_token(self, enable_token_authentication):
        """
        For using token authentication, please pass 'true'

        :param enable_token_authentication: bool
        """
        self._auth_token = enable_token_authentication
        return self

    def get_session_token(self):
        """
        Returns the auth token of the current session
        """
        return self._connection.auth_token

    # - # - # - # - # - # - # - # - # - # - # - # - # Server Commands # - # - # - # - # - # - # - # - # - # - # - # - #

    def connect(self, user, password, client_id=''):
        """
        Connect to the server without opening a database

        :param user: the username of the user on the server. e.g.: 'root'
        :param password: the password of the user on the server. e.g.: 'secret_password'
        :param client_id: client's id - can be null for clients. In clustered configuration it's the distributed node ID
        as TCP host:port
        """
        return self.get_message("ConnectMessage").prepare((user, password, client_id, self._serialization_type))\
            .send().fetch_response()

    def db_count_records(self):
        """
        Returns the number of records in the currently opened database

        :return: long
        Usage::
            >>> from pyorient import OrientDB
            >>> client = OrientDB("localhost", 2424)
            >>> client.db_open('MyDatabase', 'admin', 'admin')
            >>> client.db_count_records()
            7872
        """
        return self.get_message("DbCountRecordsMessage").prepare(()).send().fetch_response()

    def db_create(self, name, type=DB_TYPE_DOCUMENT, storage=STORAGE_TYPE_PLOCAL):
        """
        Creates a database on the OrientDB instance

        :param name: the name of the database to create. Example: "MyDatabase".
        :param type: the type of the database to create. Can be either document or graph. [default: DB_TYPE_DOCUMENT]
        :param storage:  specifies the storage type of the database to create. It can be one of the supported types [default: STORAGE_TYPE_PLOCAL]:
            - STORAGE_TYPE_PLOCAL - persistent database
            - STORAGE_TYPE_MEMORY - volatile database
        :return: None
        Usage::
            >>> from pyorient import OrientDB
            >>> client = OrientDB("localhost", 2424)
            >>> client.connect('root', 'root')
            >>> client.db_create('test')
        """
        self.get_message("DbCreateMessage").prepare((name, type, storage)).send().fetch_response()

    def db_drop(self, name, type=STORAGE_TYPE_PLOCAL):
        """
        Deletes a database from the OrientDB instance
        This returns an Exception if the database does not exist on the server.

        :param name: the name of the database to drop. Example: "MyDatabase".
        :param type: the type of the database to drop. Can be either document or graph. [default: DB_TYPE_DOCUMENT]
        :return: None
        """
        self.get_message("DbDropMessage").prepare((name, type)).send().fetch_response()

    def db_exists(self, name, type=STORAGE_TYPE_PLOCAL):
        """
        Checks if a database exists on the OrientDB instance

        :param name: the name of the database to create. Example: "MyDatabase".
        :param type: the type of the database to create. Can be either document or graph. [default: DB_TYPE_DOCUMENT]
        :return: bool
        """
        return self.get_message("DbExistsMessage").prepare((name, type)).send().fetch_response()

    def db_open(self, name, user, password, type=DB_TYPE_DOCUMENT, client_id=''):
        """
        Opens a database on the OrientDB instance
        Returns the session id to being reused for all the next calls and the list of configured clusters

        :param name: database name as string. Example: "demo"
        :param user: username as string
        :param password: password as string
        :param type: string, can be DB_TYPE_DOCUMENT or DB_TYPE_GRAPH
        :param client_id: Can be null for clients. In clustered configuration is the distributed node
        :return: an array of :class:`OrientCluster <pyorient.types.OrientCluster>` object
        Usage::
          >>> import pyorient
          >>> orient = pyorient.OrientDB('localhost', 2424)
          >>> orient.db_open('asd', 'admin', 'admin')
        """
        info, clusters, nodes = self.get_message("DbOpenMessage").prepare((name, user, password, type, client_id)).send().fetch_response()

        self.version = info
        self.clusters = clusters
        self._reload_clusters()
        self.nodes = nodes
        self.update_properties()

        return self.clusters

    def db_reload(self):
        """
        Reloads current connected database

        :return: renewed array of :class:`OrientCluster <pyorient.types.OrientCluster>`
        """
        self.clusters = self.get_message("DbReloadMessage").prepare([]).send().fetch_response()
        self._reload_clusters()
        self.update_properties()
        return self.clusters

    def update_properties(self):
        """
        This method fetches the global properties from the server. The properties are used
        for deserializing based on property index if using binary serialization. This method
        should be called after any manual command that may result in modifications to the
        properties table, for example, "Create property ..." or "Create class ..." followed
        by "Create vertex set ..."
        """
        if self._serialization_type == OrientSerialization.Binary:
            self._connection._props = {x['id']: [x['name'], TYPE_MAP[x['type']]] for x in
                                       self.command("select from #0:1")[0].oRecordData['globalProperties']}

    def shutdown(self, *args):
        """
        Stops the OrientDb instance. Requires special permissions
        """
        return self.get_message("ShutdownMessage").prepare(args).send().fetch_response()

    # - # - # - # - # - # - # - # - # - # - # - # - Database Commands # - # - # - # - # - # - # - # - # - # - # - # - #

    def gremlin(self, *args):
        return self.get_message("CommandMessage").prepare((QUERY_GREMLIN,) + args).send().fetch_response()

    def command(self, *args):
        return self.get_message("CommandMessage").prepare((QUERY_CMD,) + args).send().fetch_response()

    def batch(self, *args):
        return self.get_message("CommandMessage").prepare((QUERY_SCRIPT,) + args).send().fetch_response()

    def query(self, *args):
        return self.get_message("CommandMessage").prepare((QUERY_SYNC,) + args).send().fetch_response()

    def query_async(self, *args):
        return self.get_message("CommandMessage").prepare((QUERY_ASYNC,) + args).send().fetch_response()

    def data_cluster_add(self, *args):
        return self.get_message("DataClusterAddMessage").prepare(args).send().fetch_response()

    def data_cluster_count(self, *args):
        return self.get_message("DataClusterCountMessage").prepare(args).send().fetch_response()

    def data_cluster_data_range(self, *args):
        return self.get_message("DataClusterDataRangeMessage").prepare(args).send().fetch_response()

    def data_cluster_drop(self, *args):
        return self.get_message("DataClusterDropMessage").prepare(args).send().fetch_response()

    def db_close(self, *args):
        return self.get_message("DbCloseMessage").prepare(args).send().fetch_response()

    def db_size(self, *args):
        return self.get_message("DbSizeMessage").prepare(args).send().fetch_response()

    def db_list(self, *args):
        return self.get_message("DbListMessage").prepare(args).send().fetch_response()

    def record_create(self, *args):
        return self.get_message("RecordCreateMessage").prepare(args).send().fetch_response()

    def record_delete(self, *args):
        return self.get_message("RecordDeleteMessage").prepare(args).send().fetch_response()

    def record_load(self, *args):
        return self.get_message("RecordLoadMessage").prepare(args).send().fetch_response()

    def record_update(self, *args):
        return self.get_message("RecordUpdateMessage").prepare(args).send().fetch_response()

    def tx_commit(self):
        return self.get_message("TxCommitMessage")

    def get_message(self, command=None):
        try:
            if command is not None and MESSAGES[command]:
                # Import class with the class name from the messages dictionary
                _msg = __import__(MESSAGES[command], globals(), locals(), [command])

                # Get the right instance from import list
                _Message = getattr(_msg, command)
                if self._connection.auth_token != b'':
                    token = self._connection.auth_token
                else:
                    token = self._auth_token

                message_instance = _Message(self._connection).set_session_token(token)
                message_instance._push_callback = self._push_received
                return message_instance
        except KeyError as e:
            self.close()
            raise PyOrientBadMethodCallException("Unable to find command " + str(e), [])

    def _push_received(self, command_id, payload):
        # REQUEST_PUSH_RECORD	        79
        # REQUEST_PUSH_DISTRIB_CONFIG	80
        # REQUEST_PUSH_LIVE_QUERY	    81
        if command_id == 80:
            pass
