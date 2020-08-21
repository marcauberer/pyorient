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
from ..constants import RECORD_TYPE_DOCUMENT, FIELD_BYTE, RECORD_CREATE_OP
from ..otypes import OrientRecord
from ..utils import need_db_opened

#
# RECORD CREATE
#
# Create a new record. Returns the position in the cluster
#   of the new record. New records can have version > 0 (since v1.0)
#   in case the RID has been recycled.
#
# Request: (cluster-id:short)(record-content:bytes)(record-type:byte)(mode:byte)
# Response:
#   (cluster-position:long)(record-version:int)(count-of-collection-changes)
#   [(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)
#   (updated-page-index:long)(updated-page-offset:int)]*
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
        self._cluster_id = b'0'
        self._record_content = OrientRecord
        self._record_type = RECORD_TYPE_DOCUMENT
        self._mode_async = 0  # synchronous mode
        self._append((FIELD_BYTE, RECORD_CREATE_OP))

    @need_db_opened
    def prepare(self, params=None):
        pass

    def fetch_response(self):
        pass

    def set_data_segment_id(self, data_segment_id):
        pass

    def set_cluster_id(self, cluster_id):
        pass

    def set_record_content(self, record):
        pass

    def set_record_type(self, record_type):
        pass

    def set_mode_async(self):
        self._mode_async = 1
        return self
