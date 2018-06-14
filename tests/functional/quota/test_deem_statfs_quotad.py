#  Copyright (C) 2015-2018  Red Hat, Inc. <http://www.redhat.com>
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

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.quota_ops import (quota_enable, quota_disable,
                                          quota_check_deem_statfs)
from glustolibs.gluster.quota_libs import quota_fetch_daemon_pid
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['distributed-replicated', 'replicated', 'dispersed', 'distributed',
           'distributed-dispersed'],
          ['glusterfs']])
class QuotaDeemStatfsAndQuotad(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        """
        setup volume and initialize necessary variables
        """
        GlusterBaseClass.setUpClass.im_func(cls)
        g.log.info("Starting to Setup Volume")
        ret = cls.setup_volume()
        if not ret:
            raise ExecutionError("Failed to Setup_Volume")
        g.log.info("Successful in Setup Volume")

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the volume
        """
        # stopping the volume and clean up the volume
        g.log.info("Starting to Cleanup Volume")
        ret = cls.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Successful in Cleanup Volume")

        # calling GlusterBaseClass tearDownClass
        GlusterBaseClass.tearDownClass.im_func(cls)

    def test_quota_deem_statfs_quotad(self):
        """
        Verifying directory quota functionality with respect
        to the quota daemon and deem-statfs quota option with
        quota being enabled and disabled on the volume.

        * Check for quota daemon on all nodes when quota is
          not enabled on the volume. NO quota daemon process must
          be running.
        * Enable Quota on the Volume
        * Check for volume option features.quota-deem-statfs on the
          volume. It should be ON for the volume since quota was enabled.
        * Check for the quota daemon process on all nodes.
          There should be ONE quota daemon process running.
        * Disable quota on the volume.
        * Check for volume option features.quota-deem-statfs on the
          volume. It should be OFF for the volume since quota was disabled.
        * Check for the quota daemon process on all nodes.
          There should be NO quota daemon process running.
        """

        nodes = self.servers

        # Enable Quota
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Check for quota-deem-statfs on the volume
        g.log.info("Validating features.quota-deem-statfs on the volume %s",
                   self.volname)
        ret = quota_check_deem_statfs(self.mnode, self.volname)
        self.assertTrue(ret, "Failed to validate volume option "
                        "'features.quota-deem-statfs' on the volume %s"
                        % self.volname)

        # Check for the quota daemon on all nodes
        g.log.info("Validating presence of quota daemon process on all the "
                   "nodes belonging to volume %s", self.volname)
        ret, pids = quota_fetch_daemon_pid(nodes)
        self.assertTrue(ret, ("Failed to validate quotad presence on the nodes"
                              " from %s", pids))
        g.log.info("Successful in getting pids %s", pids)
        for node in pids:
            self.assertNotEqual(pids[node][0], -1, ("Failed to validate "
                                                    "quotad on the node %s"
                                                    % node))
        g.log.info("EXPECTED: One quota daemon process running after enabling "
                   "quota on the volume %s", self.volname)

        # Disable Quota
        g.log.info("Disabling quota on the volume %s", self.volname)
        ret, _, _ = quota_disable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to disable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully disabled quota on the volume %s",
                   self.volname)

        # Check for quota-deem-statfs on the volume
        g.log.info("Validating features.quota-deem-statfs on the volume %s",
                   self.volname)
        ret = quota_check_deem_statfs(self.mnode, self.volname)
        self.assertFalse(ret, "Failed to validate volume option "
                         "'features.quota-deem-statfs' on the volume %s"
                         % self.volname)

        # Check for the quota daemon on all nodes
        g.log.info("Validating presence of quota daemon process on all the "
                   "nodes belonging to volume %s", self.volname)
        ret, pids = quota_fetch_daemon_pid(nodes)
        self.assertFalse(ret, ("ONE quota daemon process running on one or "
                               "more nodes : %s" % pids))
        for node in pids:
            self.assertEqual(pids[node][0], -1, ("Quota daemon still running "
                                                 "on the node %s even after "
                                                 "disabling quota on the "
                                                 "volume" % node))
        g.log.info("EXPECTED: NO Quota daemon process is running after "
                   "disabling quota on the Volume %s", self.volname)
