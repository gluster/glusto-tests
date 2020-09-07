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

import time
import re
from glusto.core import Glusto as g
from glustolibs.gluster.nfs_ganesha_ops import (
        set_acl,
        unexport_nfs_ganesha_volume)
from glustolibs.gluster.nfs_ganesha_libs import (
    wait_for_nfs_ganesha_volume_to_get_unexported)
from glustolibs.gluster.gluster_base_class import runs_on, GlusterBaseClass
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNfsGaneshaAcls(GlusterBaseClass):
    """
        Tests to verify Nfs Ganesha v4 ACL stability
    """
    def setUp(self):
        """
        Setup Volume
        """
        self.get_super_method(self, 'setUp')()

        # Setup and mount volume
        g.log.info("Starting to setip and mount volume %s", self.volname)
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup and mount volume %s"
                                 % self.volname)
        g.log.info("Successful in setup and mount volume %s", self.volname)

        # Enable ACL
        ret = set_acl(self.mnode, self.volname)
        if not ret:
            raise ExecutionError("Failed to enable ACL on the nfs "
                                 "ganesha cluster")
        g.log.info("Successfully enabled ACL")

    def test_nfsv4_acls(self):
        # pylint: disable=too-many-locals

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
                           "vers=4 in %s", client)
                continue

            dirname = mountpoint + "/" + "testdir_" + client
            cmd = "[ -d %s ] || mkdir %s" % (dirname, dirname)
            ret, _, _ = g.run(client, cmd)
            self.assertEqual(ret, 0, "Failed to create dir %s for running "
                             "acl test" % dirname)

            cmd = "sh %s %s" % (test_acl_file, dirname)
            ret, out, _ = g.run(client, cmd)
            self.assertEqual(ret, 0, ("Failed to execute acl test on %s"
                                      % client))

            g.log.info("ACL test output in %s : %s", client, out)
            acl_output = out.split('\n')[:-1]
            for output in acl_output:
                match = re.search("^OK.*", output)
                if match is None:
                    self.assertTrue(False, "Unexpected behaviour in acl "
                                    "functionality in %s" % client)

            cmd = "rm -rf %s" % dirname
            ret, _, _ = g.run(client, cmd)
            self.assertEqual(ret, 0, "Failed to remove dir %s after running "
                             "acl test" % dirname)

    def tearDown(self):

        # Disable ACL
        ret = set_acl(self.mnode, self.volname, acl=False,
                      do_refresh_config=True)
        if not ret:
            raise ExecutionError("Failed to disable ACL on nfs "
                                 "ganesha cluster")
        # Unexport volume
        unexport_nfs_ganesha_volume(self.mnode, self.volname)
        ret = wait_for_nfs_ganesha_volume_to_get_unexported(self.mnode,
                                                            self.volname)
        if not ret:
            raise ExecutionError("Volume %s is not unexported." % self.volname)

        # Unmount and cleanup Volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if ret:
            g.log.info("Successfull unmount and cleanup of volume")
        else:
            raise ExecutionError("Failed to unmount and cleanup volume")
