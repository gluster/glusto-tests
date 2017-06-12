#  Copyright (C) 2016-2017  Red Hat, Inc. <http://www.redhat.com>
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

""" Description:
        Test Cases in this module tests the nfs ganesha version 4
        ACL functionality.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.nfs_ganesha_libs import NfsGaneshaVolumeBaseClass
from glustolibs.gluster.nfs_ganesha_ops import enable_acl, disable_acl
from glustolibs.gluster.exceptions import ExecutionError
import time
import re


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNfsGaneshaAcls(NfsGaneshaVolumeBaseClass):
    """
        Tests to verify Nfs Ganesha v4 ACL stability
    """

    @classmethod
    def setUpClass(cls):
        NfsGaneshaVolumeBaseClass.setUpClass.im_func(cls)

    def setUp(self):
        ret = enable_acl(self.servers[0], self.volname)
        if not ret:
            raise ExecutionError("Failed to enable ACL on the nfs "
                                 "ganesha cluster")

    def test_nfsv4_acls(self):

        source_file = ("/usr/share/glustolibs/io/scripts/nfs_ganesha/"
                       "nfsv4_acl_test.sh")
        test_acl_file = "/tmp/nfsv4_acl_test.sh"

        for server in self.servers:
            g.upload(server, source_file, "/tmp/", user="root")

            cmd = ("export ONLY_CREATE_USERS_AND_GROUPS=\"yes\";sh %s %s"
                   % (test_acl_file, "/tmp"))
            ret, _, _ = g.run(server, cmd)
            self.assertEqual(ret, 0, ("Failed to create users and groups "
                                      "for running acl test in server %s"
                                      % server))
        time.sleep(5)

        for client in self.clients:
            g.upload(client, source_file, "/tmp/", user="root")
            option_flag = 0
            for mount in self.mounts:
                if mount.client_system == client:
                    mountpoint = mount.mountpoint
                    if "vers=4" not in mount.options:
                        option_flag = 1
                    break

            if option_flag:
                g.log.info("This acl test required mount option to be "
                           "vers=4 in %s" % client)
                continue

            dirname = mountpoint + "/" + "testdir_" + client
            cmd = "[ -d %s ] || mkdir %s" % (dirname, dirname)
            ret, _, _ = g.run(client, cmd)
            self.assertEqual(ret, 0, ("Failed to create dir %s for running "
                             "acl test" % dirname))

            cmd = "sh %s %s" % (test_acl_file, dirname)
            ret, out, _ = g.run(client, cmd)
            self.assertEqual(ret, 0, ("Failed to execute acl test on %s"
                                      % client))

            g.log.info("ACL test output in %s : %s" % (client, out))
            acl_output = out.split('\n')[:-1]
            for output in acl_output:
                match = re.search("^OK.*", output)
                if match is None:
                    self.assertTrue(False, ("Unexpected behaviour in acl "
                                    "functionality in %s" % client))

            cmd = "rm -rf %s" % dirname
            ret, _, _ = g.run(client, cmd)
            self.assertEqual(ret, 0, ("Failed to remove dir %s after running "
                             "acl test" % dirname))

    def tearDown(self):
        ret = disable_acl(self.servers[0], self.volname)
        if not ret:
            raise ExecutionError("Failed to disable ACL on nfs "
                                 "ganesha cluster")

    @classmethod
    def tearDownClass(cls):
        NfsGaneshaVolumeBaseClass.tearDownClass.im_func(cls)
