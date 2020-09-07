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
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from random import choice
import string

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.volume_ops import set_volume_options


@runs_on([['dispersed', 'distributed-dispersed'], ['glusterfs']])
class TestDisperseEagerLock(GlusterBaseClass):
    def setUp(self):
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")

    @staticmethod
    def get_random_string(chars, str_len=4):
        return ''.join((choice(chars) for _ in range(str_len)))

    def test_disperse_eager_lock_cli(self):
        """
        Testcase Steps:
        1.Create an EC volume
        2.Set the eager lock option by turning
          on disperse.eager-lock by using different inputs:
          - Try non boolean values(Must fail)
          - Try boolean values
        """
        # Set the eager lock option by turning
        # on disperse.eager-lock by using different inputs
        key = 'disperse.eager-lock'

        # Set eager lock option with non-boolean value
        for char_type in (string.ascii_letters, string.punctuation,
                          string.printable, string.digits):
            temp_val = self.get_random_string(char_type)
            value = "{}".format(temp_val)
            ret = set_volume_options(self.mnode, self.volname, {key: value})
            self.assertFalse(ret, "Unexpected: Erroneous value {}, to option "
                             "{} should result in failure".format(value, key))

        # Set eager lock option with boolean value
        for value in ('1', '0', 'off', 'on', 'disable', 'enable'):
            ret = set_volume_options(self.mnode, self.volname, {key: value})
            self.assertTrue(ret, "Unexpected: Boolean value {},"
                            " to option {} shouldn't result in failure"
                            .format(value, key))
        g.log.info("Only Boolean values are accpeted by eager lock.")

    def tearDown(self):
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
