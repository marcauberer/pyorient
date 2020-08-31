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
from .constants import SOCK_CONN_TIMEOUT, FIELD_SHORT, SUPPORTED_PROTOCOL, ERROR_ON_NEWER_PROTOCOL, MESSAGES
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

    def detect_server_disconnect(self, server_conn_timeout=SOCK_CONN_TIMEOUT):
        # Trick to detect server disconnection to prevent this:
        # https://docs.python.org/2/howto/sockets.html#when-sockets-die
        try:
            # As soon as the connection crashes, this raises an exception
            return select.select([], [self.socket], [self.socket], server_conn_timeout)
        except select.error as e:
            # Connection crash, try to shutdown connection gracefully
            self.connected = False
            self.socket.close()
            raise e

    def write(self, buff):
        # Call method to detect server disconnect
        _, ready_to_write, in_error = self.detect_server_disconnect(1)

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
            # Call method to detect server disconnect
            ready_to_read, _, in_error = self.detect_server_disconnect()

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

    def get_message(self, command=None):
        try:
            if command is not None and MESSAGES[command]:
                # Import class with the class name from the messages dictionary
                _msg = __import__(
                    MESSAGES[command],
                    globals(),
                    locals(),
                    [command]
                )

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

    @staticmethod
    def _push_received(command_id, payload):
        # REQUEST_PUSH_RECORD	        79
        # REQUEST_PUSH_DISTRIB_CONFIG	80
        # REQUEST_PUSH_LIVE_QUERY	    81
        if command_id == 80:
            pass
