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
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online)
from glustolibs.gluster.brick_libs import (select_bricks_to_bring_offline,
                                           bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           wait_for_bricks_to_be_online)
from glustolibs.gluster.heal_libs import (
    wait_for_self_heal_daemons_to_be_online,
    monitor_heal_completion)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (collect_mounts_arequal, validate_io_procs)


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'cifs', 'nfs']])
class TestSelfHeal(GlusterBaseClass):
    """
    Description:
        Arbiter Test cases related to
        healing in default configuration of the volume
    """

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Overriding the volume type to specifically test the volume type
        # Change from distributed-replicated to arbiter
        if cls.volume_type == "distributed-replicated":
            cls.volume['voltype'] = {
                'type': 'distributed-replicated',
                'dist_count': 2,
                'replica_count': 3,
                'arbiter_count': 1,
                'transport': 'tcp'}

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on mounts",
                   cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        If test method failed before validating IO, tearDown waits for the
        IO's to complete and checks for the IO exit status

        Cleanup and umount volume
        """

        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_self_heal_algorithm_full_daemon_off(self):
        """""
        Description:-
        Checking healing when algorithm is set to "full" and self heal daemon
        is "off".
        """""
        # pylint: disable=too-many-statements

        # Setting volume option of self heal & algorithm
        options = {"metadata-self-heal": "disable",
                   "entry-self-heal": "disable",
                   "data-self-heal": "disable",
                   "data-self-heal-algorithm": "full",
                   "self-heal-daemon": "off"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, "Failed to set the volume options %s" % options)
        g.log.info(" Volume set options success")

        # Select bricks to bring down
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = bricks_to_bring_offline_dict['volume_bricks']
        g.log.info("Bringing bricks: %s offline", bricks_to_bring_offline)

        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, "Failed to bring bricks: %s offline"
                        % bricks_to_bring_offline)
        g.log.info("Successful in bringing bricks: %s offline",
                   bricks_to_bring_offline)

        # Validate if bricks are offline
        g.log.info("Validating if bricks: %s are offline",
                   bricks_to_bring_offline)
        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, "Not all the bricks in list:%s are offline"
                        % bricks_to_bring_offline)
        g.log.info("Successfully validated that bricks %s are all offline",
                   bricks_to_bring_offline)

        # IO on the mount point
        all_mounts_procs = []
        g.log.info("Creating Files on %s:%s", self.mounts[0].client_system,
                   self.mounts[0].mountpoint)
        cmd = ("cd %s ;for i in `seq 1 100` ;"
               "do dd if=/dev/urandom of=file$i bs=1M "
               "count=1;done"
               % self.mounts[0].mountpoint)
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # Collecting Arequal before bring the bricks up
        g.log.info("Collecting Arequal before the bring of bricks down")
        result_before = collect_mounts_arequal(self.mounts)

        # Turning self heal daemon ON
        optionstwo = {"self-heal-daemon": "on"}
        ret = set_volume_options(self.mnode, self.volname, optionstwo)
        self.assertTrue(ret, "Failed to turn self-heal ON")
        g.log.info("Volume set options %s: success", optionstwo)

        # Bring bricks online
        g.log.info("Bring bricks: %s online", bricks_to_bring_offline)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, "Failed to bring bricks: %s online"
                        % bricks_to_bring_offline)
        g.log.info("Successfully brought all bricks:%s online",
                   bricks_to_bring_offline)

        # Waiting for bricks to come online
        g.log.info("Waiting for brick process to come online")
        ret = wait_for_bricks_to_be_online(self.mnode,
                                           self.volname,
                                           timeout=30)
        self.assertTrue(ret, "bricks didn't come online after adding bricks")
        g.log.info("Bricks are online")

        # Verifying all bricks online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, "Volume %s : All process are not online"
                        % self.volname)
        g.log.info("Volume %s : All process are online", self.volname)

        # Wait for self heal processes to come online
        g.log.info("Wait for selfheal process to come online")
        ret = wait_for_self_heal_daemons_to_be_online(self.mnode,
                                                      self.volname,
                                                      timeout=300)
        self.assertTrue(ret, "Self-heal process are not online")
        g.log.info("All self heal process are online")

        # Wait for self-heal to complete
        g.log.info("Wait for self-heal to complete")
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, "Self heal didn't complete even after waiting "
                        "for 20 minutes. 20 minutes is too much a time for "
                        "current test workload")
        g.log.info("self-heal is successful after replace-brick operation")

        # arequal after healing
        g.log.info("Collecting Arequal before the bring of bricks down")
        result_after = collect_mounts_arequal(self.mounts)

        # Comparing the results
        g.log.info("comparing both the results")
        self.assertEqual(result_before, result_after, "Arequals are not equal")
