#  Copyright (C) 2015-2016  Red Hat, Inc. <http://www.redhat.com>
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

from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)

""" Example1: Using GlusterBaseClass
"""


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class TestUsingGlusterBaseClass(GlusterBaseClass):
    """Use GlusterBaseClass
    """
    @classmethod
    def setUpClass(cls):
        """setUpClass. This will be executed once per class.
        """
        # Calling GlusterBaseClass setUpClass. This will read all the
        # Variables from the g.config and will assign values to variables to
        # Use in the tests
        GlusterBaseClass.setUpClass.im_func(cls)

        # Add test class setup code here.

    def setUp(self):
        """setUp before the test
        """
        # Calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        # Add test setup code here

    def test1(self):
        pass

    def test2(self):
        pass

    def test3(self):
        pass

    def tearDown(self):
        """teardown after the test
        """
        # Add test teardown code here

        # Calling GlusterBaseClass teardown
        GlusterBaseClass.tearDown.im_func(self)

    @classmethod
    def tearDownClass(cls):
        """tearDownClass. This will be executed once per class.
        """
        # Add test class teardown code here

        # Calling GlusterBaseClass tearDownClass.
        GlusterBaseClass.tearDownClass.im_func(cls)
