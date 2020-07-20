#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

import string
from random import choice
from sys import version_info

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import set_volume_options


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestChangeReserveLimit(GlusterBaseClass):
    """
    Test to validate behaviour of 'storage.reserve' option on supplying
    erroneous values.
    """

    # pylint: disable=redefined-builtin
    def setUp(self):
        self.get_super_method(self, 'setUp')()
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Failed to create the volume")
        g.log.info("Created volume successfully")

    def tearDown(self):
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup the volume")
        g.log.info("Successfully cleaned the volume")
        self.get_super_method(self, 'tearDown')()

    @staticmethod
    def get_random_string(chars, str_len=4):
        return ''.join((choice(chars) for _ in range(str_len)))

    def test_change_reserve_limit_to_wrong_value(self):
        """
        Test Case:
        1) Create and start a distributed-replicated volume.
        2) Give different inputs to the storage.reserve volume set options
        3) Validate the command behaviour on wrong inputs
        """

        # Creation of random data for storage.reserve volume option
        # Data has: alphabets, numbers, punctuations and their combinations
        key = 'storage.reserve'

        # Make `unicode` compatible with py2/py3
        if version_info.major == 3:
            unicode = str

        for char_type in (string.ascii_letters, string.punctuation,
                          string.printable):

            # Remove quotes from the generated string
            temp_val = self.get_random_string(char_type)
            temp_val = unicode(temp_val).translate({39: None, 35: None})
            value = "'{}'".format(temp_val)
            ret = set_volume_options(self.mnode, self.volname, {key: value})
            self.assertFalse(
                ret, "Unexpected: Erroneous value {}, to option "
                "{} should result in failure".format(value, key))

        # Passing an out of range value
        for value in ('-1%', '-101%', '101%', '-1', '-101'):
            ret = set_volume_options(self.mnode, self.volname, {key: value})
            self.assertFalse(
                ret, "Unexpected: Erroneous value {}, to option "
                "{} should result in failure".format(value, key))
