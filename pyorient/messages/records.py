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
from ..constants import RECORD_TYPE_DOCUMENT, FIELD_BYTE, FIELD_INT, FIELD_LONG, FIELD_SHORT, FIELD_BOOLEAN,\
    FIELD_STRING, RECORD_CREATE_OP, RECORD_UPDATE_OP, RECORD_TYPES
from ..exceptions import PyOrientConnectionException, PyOrientBadMethodCallException
from ..otypes import OrientRecord
from ..utils import need_db_opened, parse_cluster_id, parse_cluster_position


#
# RECORD CREATE
#
# Create a new record. Returns the position in the cluster
#   of the new record. New records can have version > 0 (since v1.0)
#   in case the RID has been recycled.
#
# Request: (cluster-id:short)(record-content:bytes)(record-type:byte)(mode:byte)
# Response: (cluster-position:long)(record-version:int)(count-of-collection-changes)[(uuid-most-sig-bits:long)
#   (uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]*
#
# record-type is:
# - 'b': raw bytes
# - 'f': flat data
# - 'd': document
#
# and mode is:
# - 0 = synchronous (default mode waits for the answer)
# - 1 = asynchronous (don't need an answer)
#
# The last part of response is referred to RidBag management.
# Take a look at the main page for more details.
#
class RecordCreateMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super(RecordCreateMessage, self).__init__(_orient_socket)

        # Initialize attributes with default values
        self._data_segment_id = -1
        self._cluster_id = b'0'
        self._record_content = OrientRecord
        self._record_type = RECORD_TYPE_DOCUMENT
        self._mode_async = 0  # synchronous mode
        self._append((FIELD_BYTE, RECORD_CREATE_OP))

    @need_db_opened
    def prepare(self, params=None):
        try:
            # Use provided data
            self.set_cluster_id(params[0])
            self._record_content = params[1]
            self.set_record_type(params[2])  # optional
        except IndexError:
            # Use default for non existent indexes
            pass

        # Get instance of OrientRecord
        record = self._record_content
        if not isinstance(record, OrientRecord):
            record = self._record_content = OrientRecord(record)

        # Append header fields
        o_record_enc = self.get_serializer().encode(record)
        self._append((FIELD_SHORT, int(self._cluster_id)))
        self._append((FIELD_STRING, o_record_enc))
        self._append((FIELD_BYTE, self._record_type))
        self._append((FIELD_BOOLEAN, self._mode_async))

        return super(RecordCreateMessage, self).prepare()

    def fetch_response(self):
        # skip execution in case of transaction
        if self._orientSocket.in_transaction is True:
            return self

        self._append(FIELD_SHORT)  # cluster-id
        self._append(FIELD_LONG)  # cluster-position
        self._append(FIELD_INT)  # record-version
        result = super(RecordCreateMessage, self).fetch_response()

        chng = 0
        _changes = []

        try:
            chng = self._decode_field(FIELD_INT)  # Count of collection changes
        except (PyOrientConnectionException, TypeError):
            pass

        try:
            for x in range(0, chng):
                change = [
                    self._decode_field(FIELD_LONG),  # (uuid-most-sig-bits:long)
                    self._decode_field(FIELD_LONG),  # (uuid-least-sig-bits:long)
                    self._decode_field(FIELD_LONG),  # (updated-file-id:long)
                    self._decode_field(FIELD_LONG),  # (updated-page-index:long)
                    self._decode_field(FIELD_INT)  # (updated-page-offset:int)
                ]
                _changes.append(change)
        except (PyOrientConnectionException, TypeError):
            pass

        rid = "#" + str(result[0]) + ":" + str(result[1])
        version = result[2]
        self._record_content.update(__version=version, __rid=rid)

        return self._record_content  # [ self._record_content, _changes ]

    def set_data_segment_id(self, data_segment_id):
        self._data_segment_id = data_segment_id
        return self

    def set_cluster_id(self, cluster_id):
        self._cluster_id = parse_cluster_id(cluster_id)
        return self

    def set_record_content(self, record):
        self._record_content = record
        return self

    def set_record_type(self, record_type):
        if record_type in RECORD_TYPES:
            # user choice storage if present
            self._record_type = record_type
        else:
            raise PyOrientBadMethodCallException(record_type + ' is not a valid record type', [])
        return self

    def set_mode_async(self):
        self._mode_async = 1
        return self


#
# RECORD UPDATE
#
# Update a record. Returns the new version of the affected record.
# Request: (cluster-id:short)(cluster-position:long)(update-content:boolean)(record-content:bytes)(record-version:int)
#   (record-type:byte)(mode:byte)
# Response: (record-version:int)(count-of-collection-changes)[(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)
#   (updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]*
#
# Where record-type is:
# 'b': raw bytes
# 'f': flat data
# 'd': document
#
# and record-version policy is:
# '-1': Document update, version increment, no version control.
# '-2': Document update, no version control nor increment.
# '-3': Used internal in transaction rollback (version decrement).
# '>-1': Standard document update (version control).
#
# and mode is:
# 0 = synchronous (default mode waits for the answer)
# 1 = asynchronous (don't need an answer)
#
# and update-content is:
# true - content of record has been changed and content should
#   be updated in storage
# false - the record was modified but its own content has
#   not been changed. So related collections (e.g. rig-bags) have to
#   be updated, but record version and content should not be.
#
# The last part of response is referred to RidBag management.
# Take a look at the main page for more details.
#
class RecordUpdateMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super(RecordUpdateMessage, self).__init__(_orient_socket)

        # Initialize attributes with default values
        self._data_segment_id = -1
        self._cluster_id = b'0'
        self._cluster_position = 0
        self._record_content = ''

        # True:  content of record has been changed
        #        and content should be updated in storage
        # False: the record was modified but its own
        #        content has not been changed.
        #        So related collections (e.g. rid-bags) have to be updated, but
        #        record version and content should not be.
        self._update_content = True
        self._record_version_policy = -1  # > -1 default Standard document update (version control)
        self._record_version = -1  # Used for transactions
        self._record_type = RECORD_TYPE_DOCUMENT
        self._mode_async = 0  # means synchronous mode
        self._append((FIELD_BYTE, RECORD_UPDATE_OP))

    @need_db_opened
    def prepare(self, params=None):
        try:
            # Use provided data
            self.set_cluster_id(params[0])
            self.set_cluster_position(params[1])
            self._record_content = params[2]
            # Optionals
            self._record_version = params[3]  # Optional|Needed for transaction
            self.set_record_type(params[4])  # Optional
            self._record_version_policy = params[5]  # Optional
            self._mode_async = params[6]  # Optional
            self._update_content = params[7]  # Optional
        except IndexError:
            # Use default for non existent indexes
            pass

        # Get instance of OrientRecord
        record = self._record_content
        if not isinstance(record, OrientRecord):
            record = self._record_content = OrientRecord(record)

        # Append header field
        o_record_enc = self.get_serializer().encode(record)
        self._append((FIELD_SHORT, int(self._cluster_id)))
        self._append((FIELD_LONG, int(self._cluster_position)))
        self._append((FIELD_BOOLEAN, self._update_content))
        self._append((FIELD_STRING, o_record_enc))
        self._append((FIELD_INT, int(self._record_version_policy)))
        self._append((FIELD_BYTE, self._record_type))
        self._append((FIELD_BOOLEAN, self._mode_async))

        return super(RecordUpdateMessage, self).prepare()

    def fetch_response(self):
        # skip execution in case of transaction
        if self._orientSocket.in_transaction is True:
            return self

        self._append(FIELD_INT)  # record-version
        result = super(RecordUpdateMessage, self).fetch_response()

        chng = 0
        _changes = []
        try:
            chng = self._decode_field(FIELD_INT)  # count of collection changes
        except (PyOrientConnectionException, TypeError):
            pass

        try:
            for x in range(0, chng):
                change = [
                    self._decode_field(FIELD_LONG),  # (uuid-most-sig-bits:long)
                    self._decode_field(FIELD_LONG),  # (uuid-least-sig-bits:long)
                    self._decode_field(FIELD_LONG),  # (updated-file-id:long)
                    self._decode_field(FIELD_LONG),  # (updated-page-index:long)
                    self._decode_field(FIELD_INT)  # (updated-page-offset:int)
                ]
                _changes.append(change)
        except IndexError:
            # append an empty field
            result.append(None)

        self._record_content.update(__version=result[0])

        return [self._record_content, chng, _changes]

    def set_data_segment_id(self, data_segment_id):
        self._data_segment_id = data_segment_id
        return self

    def set_cluster_id(self, cluster_id):
        self._cluster_id = parse_cluster_id(cluster_id)
        return self

    def set_cluster_position(self, _cluster_position):
        self._cluster_position = parse_cluster_position(_cluster_position)
        return self

    def set_record_content(self, record):
        self._record_content = record
        return self

    def set_record_type(self, record_type):
        if record_type in RECORD_TYPES:
            # user choice storage if present
            self._record_type = record_type
        else:
            raise PyOrientBadMethodCallException(record_type + ' is not a valid record type', [])
        return self

    def set_mode_async(self):
        self._mode_async = 1
        return self

    def set_record_version_policy(self, _policy):
        self._record_version_policy = _policy
        return self

    def set_no_update_content(self):
        self._update_content = False
        return self


