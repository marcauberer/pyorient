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

from ..constants import CONNECT_OP, FIELD_BYTE, FIELD_BOOLEAN, FIELD_STRING, FIELD_STRINGS, FIELD_SHORT, NAME, VERSION,\
    SUPPORTED_PROTOCOL
from ..utils import need_connected
from .base import BaseMessage


class ConnectMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super(ConnectMessage, self).__init__(_orient_socket)
        # Initialize attributes with default values
        self._user = ''
        self._pass = ''
        self._client_id = ''
        self._need_token = False

        # Attach handshake headers
        super(ConnectMessage, self).execute_handshake()

        self._append((FIELD_BYTE, CONNECT_OP))

    def prepare(self, params=None):
        if isinstance(params, tuple) or isinstance(params, list):
            try:
                # Use provided credentials
                self._user = params[0]
                self._pass = params[1]
                self._client_id = params[2]
            except IndexError:
                # Use default credentials
                pass

        # Append header fields
        self._append((FIELD_STRING, self._user))
        self._append((FIELD_STRING, self._pass))
        # self._append((FIELD_STRINGS, [NAME, VERSION]))
        # self._append((FIELD_SHORT, SUPPORTED_PROTOCOL))
        # self._append((FIELD_STRING, self._client_id))
        # self._append((FIELD_STRING, self._orientSocket.serialization_type))
        # self._append((FIELD_BOOLEAN, self._request_token))
        # self._append((FIELD_BOOLEAN, True))  # support-push
        # self._append((FIELD_BOOLEAN, True))  # collect-stats
        # self._append((FIELD_STRING, self._user))
        # self._append((FIELD_STRING, self._pass))

        return super(ConnectMessage, self).prepare()

    def fetch_response(self):
        pass

    def set_user(self, user):
        """
        Builder method for setting user
        """
        self._user = user
        return self

    def set_pass(self, _pass):
        """
        Builder method for setting password
        """
        self._pass = _pass
        return self

    def set_client_id(self, _cid):
        """
        Builder method for setting client id
        """
        self._client_id = _cid
        return self


class ShutdownMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super(ShutdownMessage, self).__init__(_orient_socket)
        # Initialize attributes with default values
        self._user = ''
        self._pass = ''

    @need_connected
    def prepare(self, params=None):
        if isinstance(params, tuple) or isinstance(params, list):
            try:
                # Use provided credentials
                self._user = params[0]
                self._pass = params[1]
            except IndexError:
                # Use default credentials
                pass
        self._append((FIELD_STRING, [self._user, self._pass]))
        return super(ShutdownMessage, self).prepare()

    def fetch_response(self):
        return super(ShutdownMessage, self).fetch_response()

    def set_user(self, _user):
        """
        Builder method for setting user
        """
        self._user = _user
        return self

    def set_pass(self, _pass):
        """
        Builder method for setting password
        """
        self._pass = _pass
        return self
