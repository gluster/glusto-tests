#  Copyright (C) 2017-2019  Red Hat, Inc. <http://www.redhat.com>
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

"""
        Description:
        Test Cases in this module related to test self heal
        deamon and quota daemon status after reboot.
"""
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.quota_ops import quota_enable, quota_limit_usage
from glustolibs.gluster.volume_ops import get_volume_status
from glustolibs.misc.misc_libs import reboot_nodes_and_wait_to_come_online
from glustolibs.gluster.gluster_init import is_glusterd_running


@runs_on([['replicated', 'distributed-replicated'], ['glusterfs']])
class TestSelfHealDeamonQuotaDeamonAfterReboot(GlusterBaseClass):
    def setUp(self):
        GlusterBaseClass.setUpClass.im_func(self)

        # checking for peer status from every node
        ret = self.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Servers are not in peer probed state")

        # Creating Volume
        g.log.info("Started creating volume")
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Volume creation failed: %s" % self.volname)
        g.log.info("Volume created successfully : %s", self.volname)

    def tearDown(self):
        # stopping the volume and Cleaning up the volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to Cleanup the Volume %s"
                                 % self.volname)
        g.log.info("Volume deleted successfully %s", self.volname)

    def is_daemon_process_running(self):
        """
            function for checking daemon process.
        """
        vol_status_shd_pid_list = []
        vol_status_quotad_pid_list = []
        g.log.info("Total self-heal and quota daemon process should be %d for "
                   "%d nodes", len(self.servers) * 2, len(self.servers))

        # Getting vol status in dictonary format
        vol_status = get_volume_status(self.mnode, self.volname)

        # Getting self-heal daemon and quota daemon pids of every host from
        # gluster volume status
        for server in self.servers:
            shd_pid = \
                (vol_status[self.volname][server]['Self-heal Daemon']['pid'])
            quota_pid = \
                (vol_status[self.volname][server]['Quota Daemon']['pid'])
            vol_status_quotad_pid_list.append(quota_pid)
            vol_status_shd_pid_list.append(shd_pid)

        g.log.info("shd list from get volume status: %s",
                   vol_status_shd_pid_list)
        g.log.info("quotad list from get volume status: %s",
                   vol_status_quotad_pid_list)

        sh_daemon_list = []
        quotad_list = []

        # Finding and Storing all hosts self heal daemon
        # in to sh_daemon_list, all
        # host quota daemon into quotad_list list using ps command
        for host in self.servers:
            ret, out, _ = g.run(host, "ps -eaf | grep glustershd | "
                                      "grep -v grep | awk '{ print $2 }'")
            sh_daemon_list.append(out.strip())
            ret, out, _ = g.run(host, "ps -eaf | grep quotad | grep -v grep |"
                                      " awk '{ print $2 }'")
            quotad_list.append(out.strip())
        g.log.info("shd list :%s", sh_daemon_list)
        g.log.info("quotad list :%s", quotad_list)

        # Checking in all hosts quota daemon and self heal daemon is
        # running or not
        # Here comparing the list of daemons got from ps command and
        # list of daemons got from vol status command,
        # all daemons should match from both the list
        ret = cmp(sorted(sh_daemon_list + quotad_list),
                  sorted(vol_status_shd_pid_list + vol_status_quotad_pid_list))
        if ret == 0:
            ps_daemon_len = len(sh_daemon_list + quotad_list)
            vol_status_daemon_len = len((vol_status_shd_pid_list +
                                         vol_status_quotad_pid_list))
            return bool(ps_daemon_len == len(self.servers) * 2 |
                        vol_status_daemon_len == len(self.servers) * 2)

        return False

    def test_daemons_after_reboot(self):
        '''
        Creating volume then performing FUSE mount
        then enable quota to that volume, then set quota
        limit to that volume then perform a reboot and check
        the selfheal daemon and quota daemon running or not
        after reboot
        '''

        # Enabling quota to volume
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to enable quota on volume : "
                                 "%s" % self.volname)
        g.log.info("quota enabled successfully on volume: %s", self.volname)

        # Setting quota limit to volume
        ret, _, _ = quota_limit_usage(
            self.mnode,
            self.volname,
            path='/',
            limit='1GB',
            soft_limit='')
        self.assertEqual(ret, 0, "Quota limit set failed "
                                 "on volume : %s" % self.volname)

        ret, _ = reboot_nodes_and_wait_to_come_online(self.servers[1])
        self.assertTrue(ret, "Failed to reboot the node %s"
                        % self.servers[1])
        g.log.info("Node %s rebooted successfully", self.servers[1])

        # Checking glusterd status and peer status afte reboot of server
        count = 0
        while count < 100:
            ret = is_glusterd_running(self.servers[1])
            if not ret:
                ret = self.validate_peers_are_connected()
                if ret:
                    g.log.info("glusterd is running and all peers are in "
                               "connected state")
                    break
            count += 1
            sleep(5)
        self.assertEqual(count, 100,
                         "Either glusterd is not runnig or peers are "
                         "not in connected state ")

        # Checks self heal daemon and quota daemon process running or not
        ret = self.is_daemon_process_running()
        self.assertTrue(ret, "failed to run self-heal and quota daemon "
                             "processs on all hosts")
        g.log.info("self-heal and quota daemons are running on all "
                   "hosts successfully")
