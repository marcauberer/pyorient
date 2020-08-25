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

import unittest
# import os
import pyorient

# os.environ['DEBUG'] = "0"
# os.environ['DEBUG_VERBOSE'] = "0"


class GraphUsageTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(GraphUsageTestCase, self).__init__(*args, **kwargs)
        # Set attributes to default values
        self.client = None
        self.cluster_info = None

    def setUp(self):
        self.client = pyorient.OrientDB("localhost", 2424)
        self.client.connect("root", "root")
        print("Session token: " + str(self.client.get_session_token()))
        # TODO: Incomplete method

    def testGraph(self):
        # Create Vertex 'Animal'
        #self.client.command("create class Animal extends V")
        # Insert a value
        #self.client.command("insert into Animal set name = 'rat', specie = 'rodent'")
        self.client.close()


if __name__ == '__main__':
    unittest.main()

