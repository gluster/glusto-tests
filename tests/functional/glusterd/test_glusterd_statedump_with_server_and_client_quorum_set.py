#  Copyright (C) 2021 Red Hat, Inc. <http://www.redhat.com>
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

"""Test Description:
    Check glusterd statedump when server, client quorum is set on volumes.
"""

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.quota_ops import quota_enable
from glustolibs.gluster.volume_libs import cleanup_volume, setup_volume
from glustolibs.gluster.volume_ops import (set_volume_options, get_volume_list,
                                           volume_start, get_volume_options)
from glustolibs.gluster.mount_ops import (mount_volume)
from glustolibs.gluster.peer_ops import (peer_detach_servers,
                                         peer_probe_servers)
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           get_all_bricks)
from glustolibs.gluster.gluster_init import (stop_glusterd, restart_glusterd,
                                             wait_for_glusterd_to_start)


@runs_on([['replicated', 'distributed-replicated'], ['glusterfs']])
class TestGlusterdStatedumpWhenQuorumSetOnVolumes(GlusterBaseClass):
    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Volume creation failed: %s"
                                 % self.volname)

        # Remove all stale/old statedump files
        cmd = "rm -rf /var/run/gluster/glusterdump.*"
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to clear out previous "
                         "statedump files")

    def tearDown(self):
        # Restart glusterd on nodes for which it was stopped
        ret = restart_glusterd(self.servers[3:5])
        if not ret:
            raise ExecutionError("Failed to restart glusterd on nodes: %s"
                                 % self.servers[3:5])

        # Wait for glusterd to be online and validate it's running.
        ret = wait_for_glusterd_to_start(self.servers[3:5])
        if not ret:
            raise ExecutionError("Glusterd not up on the servers: %s" %
                                 self.servers[3:5])

        # clean up all volumes
        vol_list = get_volume_list(self.mnode)
        if vol_list is None:
            raise ExecutionError("Failed to get the volume list")

        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Unable to delete volume %s" % volume)
            g.log.info("Volume deleted successfully : %s", volume)

        # Peer probe detached servers
        ret = peer_probe_servers(self.mnode, self.servers[1:3])
        if not ret:
            raise ExecutionError("Failed to probe detached "
                                 "servers %s" % self.servers[1:3])

        # Remove all the statedump files created in the test
        cmd = "rm -rf /var/run/gluster/glusterdump.*"
        ret, _, _ = g.run(self.mnode, cmd)
        if ret:
            raise ExecutionError("Failed to clear out the statedump files")

        self.get_super_method(self, 'tearDown')()

    def _stop_gluster(self, server):
        """
        Stop glusterd on a server
        """
        ret = stop_glusterd(server)
        self.assertTrue(ret, "Failed to stop glusterd on node:%s" % server)
        g.log.info("Successfully stopped glusterd process on node:%s", server)

    def _set_option_for_volume(self, volume, option):
        """
        Set an option for a volume
        """
        ret = set_volume_options(self.mnode, volume, option)
        self.assertTrue(ret, "Failed to set option:value %s on volume %s"
                        % (option, volume))
        g.log.info("Successfully set option:value %s for volume %s",
                   option, volume)

    def _get_option_value_for_volume(self, option):
        """
        Get value of an option
        """
        option_value = get_volume_options(self.mnode, 'all', option)
        self.assertIsNotNone(option_value, "Failed to get %s option" % option)
        return option_value

    def _get_statedump_of_glusterd(self, count):
        """
        Confirm if statedump is collected for glusterd process
        """
        # Get the statedump of glusterd process
        cmd = "kill -USR1 `pidof glusterd`"
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to get the statedump of glusterd"
                         " process")

        # Added sleep to compensate the creation of the statedump file
        sleep(2)

        # Get the count of statedumps created
        cmd = "ls /var/run/gluster/glusterdump.* | wc -l"
        ret, cnt, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to get the count of statedumps")

        # Confirm if the new statedump was collected or not
        self.assertEqual(int(cnt), count, "Statedump was not collected"
                         " under /var/run/gluster")

    def _get_value_statedump(self, filename_analyze, value, option):
        """
        Get value from statedump
        """
        cmd = ("grep '%s' `%s` | cut -d '=' -f 2"
               % (value, filename_analyze))
        ret, out, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to get %s from statedump" % option)
        return out

    def _analyze_statedump(self):
        """
        Analyze the statedump created
        """
        # Select the statedump file to analyze
        file_to_analyze = "ls -t /var/run/gluster/glusterdump.* | head -n 1"

        # Analyze peer hostnames
        host_str = self._get_value_statedump(file_to_analyze,
                                             "glusterd.peer*..hostname",
                                             "peer hostnames")
        hosts = host_str.split()
        self.assertEqual(len(self.servers) - 1, len(hosts), "Unexpected: All "
                         "the peers are not present in statedump")
        for host in hosts:
            found = False
            if str(host) in self.servers:
                found = True
            self.assertTrue(found, "Unexpected: Peer %s not present in"
                            " statedump" % host)
        g.log.info("All the peer's hostname is present in the statedump file")

        # Analyze the quotad status
        qtd_sts = self._get_value_statedump(file_to_analyze,
                                            "glusterd.quotad.online",
                                            "quotad status")
        self.assertEqual(int(qtd_sts), 1, "Unexpected: Quotad is not online")
        g.log.info("Quotad is online, as expected in the statedump file")

        # Analyze the op-version status
        # Get the max-op-version from statedump
        mx_op_std = self._get_value_statedump(file_to_analyze,
                                              "glusterd.max-op-version",
                                              "max-op-version")

        # Getting max-op-version
        mx_opvrsn = self._get_option_value_for_volume("cluster.max-op-version")
        self.assertEqual(int(mx_op_std),
                         int(mx_opvrsn['cluster.max-op-version']),
                         "Unexpected: max-op-version of cluster is not equal")

        # Get the current op-version from statedump
        crnt_op_std = self._get_value_statedump(file_to_analyze,
                                                "glusterd.current-op-version",
                                                "current op-version")

        # Getting current op-version
        crnt_op_vrsn = self._get_option_value_for_volume("cluster.op-version")
        self.assertEqual(int(crnt_op_std),
                         int(crnt_op_vrsn['cluster.op-version']),
                         "Unexpected: current op-version of cluster not equal")
        g.log.info("Op-version's are as expected, in the statedump file")

        # Check for clients in statedump
        cmd = ("grep 'glusterd.client.*' `%s` | wc -l" % file_to_analyze)
        ret, out, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to check for clients in statedump")
        self.assertNotEqual(int(out), 0, "Unexpected: No client is present in"
                            " statedump")
        g.log.info("Client's desciption is present as expected, in statedump")

    def test_glusterd_statedump_when_quorum_set_on_volumes(self):
        """
        Test Case:
        1. Create and start a volume
        2. Enable quota on the volume
        2. Fuse mount the volume
        3. Get the glusterd statedump and analyze the statedump
        4. Enable client-side quorum on the volume
        5. Get the glusterd statedump
        6. Delete the volume and peer detach 2 nodes
        7. Create a replica 2 volume and start it
        8. Kill the first brick of the volume
        9. Get the glusterd statedump
        10. Start the volume with force
        11. Enable server-side quorum on the volume
        12. Get the glusterd statedump
        13. Stop glusterd on one of the node
        14. Get the glusterd statedump
        15. Stop glusterd on another node
        16. Get the glusterd statedump
        """
        # pylint: disable=too-many-statements
        # Enable Quota
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Mount the volume
        ret, _, _ = mount_volume(self.volname, mtype=self.mount_type,
                                 mpoint=self.mounts[0].mountpoint,
                                 mserver=self.mnode,
                                 mclient=self.mounts[0].client_system)
        self.assertEqual(ret, 0, "Failed to mount the volume: %s"
                         % self.volname)
        g.log.info("Successfully mounted the volume: %s", self.volname)

        # Get the statedump of glusterd process
        self.dump_count = 1
        self._get_statedump_of_glusterd(self.dump_count)

        # Analyze the statedump created
        self._analyze_statedump()

        # Enable client-side quorum on volume
        option = {"cluster.quorum-type": "auto"}
        self._set_option_for_volume(self.volname, option)

        # Get the statedump of glusterd process
        self.dump_count += 1
        self._get_statedump_of_glusterd(self.dump_count)

        # Delete the volume
        ret = cleanup_volume(self.mnode, self.volname)
        self.assertTrue(ret, "Failed to delete the volume: %s" % self.volname)
        g.log.info("Successfully deleted the volume: %s", self.volname)

        # Peer detach two nodes
        ret = peer_detach_servers(self.mnode, self.servers[1:3])
        self.assertTrue(ret, "Failed to detach the servers %s"
                        % self.servers[1:3])
        g.log.info("Successfully detached peers %s", self.servers[1:3])

        # Create a new replica 2 volume in the updated cluster
        self.volume_config = {
            'name': 'test_glusterd_statedump_when_quorum_'
                    'set_on_volumes_replica-volume',
            'servers': self.servers[3:],
            'voltype': {'type': 'replicated',
                        'replica_count': 2},
        }

        # Create and start a volume
        ret = setup_volume(self.mnode, self.all_servers_info,
                           self.volume_config)
        self.assertTrue(ret, "Failed to create and start the volume: %s"
                        % self.volume_config['name'])
        g.log.info("Volume %s created and started successfully",
                   self.volume_config['name'])

        # Get the list of bricks in volume
        all_bricks = get_all_bricks(self.mnode, self.volume_config['name'])
        self.assertIsNotNone(all_bricks, "Unable to get list of bricks")

        # Kill the first brick in volume
        ret = bring_bricks_offline(self.volume_config['name'], all_bricks[0])
        self.assertTrue(ret, "Unable to bring brick %s offline"
                        % all_bricks[0])
        g.log.info("Successfully brought the brick %s offline ", all_bricks[0])

        # Get the statedump of glusterd process
        self.dump_count += 1
        self._get_statedump_of_glusterd(self.dump_count)

        # Start the volume with force
        ret, _, _ = volume_start(self.mnode, self.volume_config['name'], True)
        self.assertEqual(ret, 0, "Failed to start volume %s with force"
                         % self.volume_config['name'])
        g.log.info("Successfully started volume %s with force",
                   self.volume_config['name'])

        # Enable server-side quorum on volume
        option = {"cluster.server-quorum-type": "server"}
        self._set_option_for_volume(self.volume_config['name'], option)

        # Get the statedump of glusterd process
        self.dump_count += 1
        self._get_statedump_of_glusterd(self.dump_count)

        # Stop glusterd process on one of the node.
        self._stop_gluster(self.servers[3])

        # Get the statedump of glusterd process
        self.dump_count += 1
        self._get_statedump_of_glusterd(self.dump_count)

        # Stop glusterd process on one of the node.
        self._stop_gluster(self.servers[4])

        # Get the statedump of glusterd process
        self.dump_count += 1
        self._get_statedump_of_glusterd(self.dump_count)
