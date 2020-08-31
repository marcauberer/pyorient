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

from .base import BaseMessage
from .records import RecordUpdateMessage, RecordDeleteMessage, RecordCreateMessage
from ..exceptions import PyOrientBadMethodCallException
from ..constants import QUERY_SYNC, QUERY_ASYNC, QUERY_CMD, QUERY_SCRIPT, QUERY_GREMLIN, FIELD_BYTE, FIELD_STRING,\
    FIELD_INT, FIELD_CHAR, COMMAND_OP, QUERY_TYPES
from ..utils import need_db_opened


#
# COMMAND_OP
#
# Executes remote commands:
#
# Request: (mode:byte)(class-name:string)(command-payload-length:int)(command-payload)
# Response:
# - synchronous commands: [(synch-result-type:byte)[(synch-result-content:?)]]+
# - asynchronous commands: [(asynch-result-type:byte)[(asynch-result-content:?)]*]
#   (pre-fetched-record-size.md)[(pre-fetched-record)]*+
#
# Where the request:
#
# mode can be 'a' for asynchronous mode and 's' for synchronous mode
# class-name is the class name of the command implementation.
#   There are short form for the most common commands:
# q stands for query as idempotent command. It's like passing
#   com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
# c stands for command as non-idempotent command (insert, update, etc).
#   It's like passing com.orientechnologies.orient.core.sql.OCommandSQL
# s stands for script. It's like passing
#   com.orientechnologies.orient.core.command.script.OCommandScript.
#   Script commands by using any supported server-side scripting like Javascript command. Since v1.0.
# any other values is the class name. The command will be created via
#   reflection using the default constructor and invoking the fromStream() method against it
# command-payload is the command's serialized payload (see Network-Binary-Protocol-Commands)

# Response is different for synchronous and asynchronous request:
# synchronous:
# synch-result-type can be:
# 'n', means null result
# 'r', means single record returned
# 'l', collection of records. The format is:
# an integer to indicate the collection size
# all the records one by one
# 'a', serialized result, a byte[] is sent
# synch-result-content, can only be a record
# pre-fetched-record-size, as the number of pre-fetched records not
#   directly part of the result set but joined to it by fetching
# pre-fetched-record as the pre-fetched record content
# asynchronous:
# asynch-result-type can be:
# 0: no records remain to be fetched
# 1: a record is returned as a resultset
# 2: a record is returned as pre-fetched to be loaded in client's cache only.
#   It's not part of the result set but the client knows that it's available for later access
# asynch-result-content, can only be a record
#
class CommandMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super(CommandMessage, self).__init__(_orient_socket)

        # Initialize attributes with default values
        self._query = ''
        self._limit = 20
        self._fetch_plan = '*:0'
        self._command_type = QUERY_SYNC
        self._mod_byte = 's'
        self._append((FIELD_BYTE, COMMAND_OP))

    @need_db_opened
    def prepare(self, params=None):
        if isinstance(params, tuple) or isinstance(params, list):
            try:
                # Apply passed data
                self.set_command_type(params[0])

                self._query = params[1]
                self._limit = params[2]
                self._fetch_plan = params[3]
                self.set_callback(params[4])  # callback function use to operate over the async fetched records

            except IndexError:
                # Use default for non existent indexes
                pass

        if self._command_type == QUERY_CMD or self._command_type == QUERY_SYNC or self._command_type == QUERY_SCRIPT \
                or self._command_type == QUERY_GREMLIN:
            self._mod_byte = 's'
        else:
            if self._callback is None:
                raise PyOrientBadMethodCallException("No callback was provided.", [])
            self._mod_byte = 'a'

        _payload_definition = [(FIELD_STRING, self._command_type), (FIELD_STRING, self._query)]

        if self._command_type == QUERY_ASYNC or self._command_type == QUERY_SYNC or self._command_type == QUERY_GREMLIN:
            # a limit specified in a sql string should always override a limit parameter pass to prepare()
            if ' LIMIT ' not in self._query.upper() or self._command_type == QUERY_GREMLIN:
                _payload_definition.append((FIELD_INT, self._limit))
            else:
                _payload_definition.append((FIELD_INT, -1))

            _payload_definition.append((FIELD_STRING, self._fetch_plan))

        if self._command_type == QUERY_SCRIPT:
            _payload_definition.insert(1, (FIELD_STRING, 'sql'))

        _payload_definition.append((FIELD_INT, 0))

        payload = b''.join(self._encode_field(x) for x in _payload_definition)

        self._append((FIELD_BYTE, self._mod_byte))
        self._append((FIELD_STRING, payload))

        return super(CommandMessage, self).prepare()

    def fetch_response(self):
        # skip execution in case of transaction
        if self._orientSocket.in_transaction is True:
            return self

        # decode header only
        super(CommandMessage, self).fetch_response()

        if self._command_type == QUERY_ASYNC:
            self._read_async_records()
        else:
            return self._read_sync()

    def set_command_type(self, _command_type):
        """
        Setter method for the command type
        """
        if _command_type in QUERY_TYPES:
            # user choice if present
            self._command_type = _command_type
        else:
            raise PyOrientBadMethodCallException(_command_type + ' is not a valid command type', [])
        return self

    def set_fetch_plan(self, _fetch_plan):
        """
        Setter method for the fetch plan
        """
        self._fetch_plan = _fetch_plan
        return self

    def set_query(self, _query):
        """
        Setter method for query
        """
        self._query = _query
        return self

    def set_limit(self, _limit):
        """
        Setter method for the limit
        """
        self._limit = _limit
        return self

    def _read_sync(self):
        # type of response
        # decode body char with flag continue ( Header already read )
        response_type = self._decode_field(FIELD_CHAR)
        if not isinstance(response_type, str):
            response_type = response_type.decode()
        res = []
        if response_type == 'n':
            self._append(FIELD_CHAR)
            super(CommandMessage, self).fetch_response(True)
            # end Line \x00
            return None
        elif response_type == 'r' or response_type == 'w':
            res = [self._read_record()]
            self._append(FIELD_CHAR)
            # end Line \x00
            _res = super(CommandMessage, self).fetch_response(True)
            if response_type == 'w':
                res = [res[0].oRecordData['result']]
        elif response_type == 'a':
            self._append(FIELD_STRING)
            self._append(FIELD_CHAR)
            res = [super(CommandMessage, self).fetch_response(True)[0]]
        elif response_type == 'l':
            self._append(FIELD_INT)
            list_len = super(CommandMessage, self).fetch_response(True)[0]

            for n in range(0, list_len):
                res.append(self._read_record())

            # async-result-type can be:
            # 0: no records remain to be fetched
            # 1: a record is returned as a result set
            # 2: a record is returned as pre-fetched to be loaded in client's
            #       cache only. It's not part of the result set but the client
            #       knows that it's available for later access

            # cached_results = self._read_async_records()
            # cache = cached_results['cached']
        else:
            # this should be never happen, used only to debug the protocol
            msg = b''
            self._orientSocket.socket.setblocking(0)
            m = self._orientSocket.read(1)
            while m != "":
                msg += m
                m = self._orientSocket.read(1)

        return res
