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

import re


class OrientRecord(object):
    """
    Object that represent an Orient Document / Record
    """
    oRecordData = property(lambda self: self.__o_storage)

    def __str__(self):
        # Build string, which represents the data record
        rep = ""
        if self.__o_storage:
            rep = str(self.__o_storage)
        if self.__o_class is not None:
            rep = "'@" + str(self.__o_class) + "':" + rep + ""
        if self.__version is not None:
            rep = rep + ",'version':" + str(self.__version)
        if self.__rid is not None:
            rep = rep + ",'rid':'" + str(self.__rid) + "'"
        return '{' + rep + '}'

    @staticmethod
    def addslashes(string):
        char_list = ["\\", '"', "'", "\0", ]
        for i in char_list:
            if i in string:
                string = string.replace(i, '\\' + i)
        return string

    def __init__(self, content=None):
        # Initialize attributes with default values
        self.__rid = None
        self.__version = None
        self.__o_class = None
        self.__o_storage = {}

        if not content:
            content = {}
        for key in content.keys():
            if key == '__rid':  # Ex: select @rid, field from v_class
                self.__rid = content[key]
            elif key == '__version':  # Ex: select @rid, @version from v_class
                self.__version = content[key]
            elif key == '__o_class':
                self.__o_class = content[key]
            elif key[0:1] == '@':
                # special case dict
                # { '@my_class': { 'accommodation': 'hotel' } }
                self.__o_class = key[1:]
                for _key, _value in content[key].items():
                    if isinstance(_value, str):
                        self.__o_storage[_key] = self.addslashes(_value)
                    else:
                        self.__o_storage[_key] = _value
            elif key == '__o_storage':
                self.__o_storage = content[key]
            else:
                self.__o_storage[key] = content[key]

    def _set_keys(self, content=dict):
        for key in content.keys():
            self._set_keys(content[key])

    @property
    def _in(self):
        try:
            return self.__o_storage['in']
        except KeyError:
            return None

    @property
    def _out(self):
        try:
            return self.__o_storage['out']
        except KeyError:
            return None

    @property
    def _rid(self):
        return self.__rid

    @property
    def _version(self):
        return self.__version

    @property
    def _class(self):
        return self.__o_class

    def update(self, **kwargs):
        self.__rid = kwargs.get('__rid', None)
        self.__version = kwargs.get('__version', None)
        if self.__o_class is None:
            self.__o_class = kwargs.get('__o_class', None)


class OrientRecordLink(object):
    def __init__(self, record_link):
        # Initialize attributes with default values
        cid, pos = record_link.split(":")
        self.__link = record_link
        self.clusterID = cid
        self.recordPosition = pos

    def __str__(self):
        return self.get_hash()

    def get(self):
        return self.__link

    def get_hash(self):
        return "#%s" % self.__link


class OrientBinaryObject(object):
    """
    This will be a RidBag
    """
    def __init__(self, string):
        self.b64 = string

    def get_hash(self):
        return "_" + self.b64 + "_"

    def getBin(self):
        import base64
        return base64.b64decode(self.b64)


class OrientCluster(object):
    def __init__(self, name, cluster_id, cluster_type=None, segment=None):
        """
        Information regarding a Cluster on the Orient Server
        :param name: str name of the cluster
        :param cluster_id: int id of the cluster
        :param cluster_type: cluster type (only for version <24 of the protocol)
        :param segment: cluster segment (only for version <24 of the protocol)
        """
        # Initialize attributes with default values
        self.name = name
        self.id = cluster_id
        self.type = cluster_type
        self.segment = segment

    def __str__(self):
        return "%s: %d" % (self.name, self.id)

    def __eq__(self, other):
        return self.name == other.name and self.id == other.id

    def __ne__(self, other):
        return self.name != other.name or self.id != other.id


class OrientNode(object):
    def __init__(self, node_dict=None):
        """
        Represent a server node in a multi clusered configuration
        TODO: extends this object with different listeners if we're going to support in the driver an abstarction of the HTTP protocol, for now we are not interested in that
        :param node_dict: dict with starting configs (usaully from a db_open, db_reload record response)
        """
        # Initialize attributes with default values
        self.name = None
        self.id = None
        self.started_on = None  #: datetime object the node was started
        self.host = None  #: binary listener host
        self.port = None  #: binary lister port

        if node_dict is not None:
            self._parse_dict(node_dict)

    def _parse_dict(self, node_dict):
        self.id = node_dict['id']
        self.name = node_dict['name']
        self.started_on = node_dict['startedOn']
        binary_listener = None
        for listener in node_dict['listeners']:
            if listener['protocol'] == 'ONetworkProtocolBinary':
                binary_listener = listener
                break

        if binary_listener:
            listen = binary_listener['listen'].split(':')
            self.host = listen[0]
            self.port = listen[1]

    def __str__(self):
        return self.name


class OrientVersion(object):
    def __init__(self, release):
        """
        Object representing Orient db release Version
        :param release: String release
        """
        self.release = release  # Full version string of OrientDB release
        self.major = None  # Mayor version
        self.minor = None  # Minor version
        self.build = None  # Build number
        self.subversion = None  # Build version string
        self.parse_version(release)

    def parse_version(self, release_string):
        # Convert release string to string object
        if not isinstance(release_string, str):
            release_string = release_string.decode()

        # Split '.'
        try:
            version_info = release_string.split(".")
            self.major = version_info[0]
            self.minor = version_info[1]
            self.build = version_info[2]
        except IndexError:
            pass

        # Validate major version
        regex = re.match('.*([0-9]+).*', self.major)
        self.major = regex.group(1)

        # Split '-'
        try:
            version_info = self.minor.split("-")
            self.minor = version_info[0]
            self.subversion = version_info[1]
        except IndexError:
            pass

        # Validate build number and subversion
        try:
            regex = re.match('([0-9]+)[\.\- ]*(.*)', self.build)
            self.build = regex.group(1)
            self.subversion = regex.group(2)
        except TypeError:
            pass

        # Convert numbers to ints and version strings to strings
        self.major = int(self.major)
        self.minor = int(self.minor)
        self.build = 0 if self.build is None else int(self.build)
        self.subversion = '' if self.subversion is None else str(self.subversion)
