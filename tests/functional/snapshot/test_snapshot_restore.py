#  Copyright (C) 2016-2020 Red Hat, Inc. <http://www.redhat.com>
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
The purpose of this test is to validate restore of a snapshot.
"""


from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.volume_libs import (
    shrink_volume, verify_all_process_of_volume_are_online)
from glustolibs.gluster.volume_ops import volume_reset
from glustolibs.io.utils import validate_io_procs, get_mounts_stat
from glustolibs.gluster.snap_ops import (snap_create,
                                         snap_delete_all,
                                         snap_restore_complete,
                                         set_snap_config,
                                         get_snap_config)


@runs_on([['distributed-replicated', 'distributed-dispersed'],
          ['glusterfs']])
class SnapRestore(GlusterBaseClass):
    """
    Test for snapshot restore
    Steps :
        1. Create and start a volume
        2. Mount the volume on a client
        3. Create data on the volume (v1)
        4. Set some volume option
        5. Take snapshot of volume
        6. Create some more data on volume (v2)
        7. Reset volume option
        8. Remove brick/bricks
        9. Stop volume
        10. Restore snapshot
        11. Start and mount volume
        12. Validate data on volume (v1)
        13. Validate volume option
        14. Validate bricks after restore
        15. Create snapshot of restored volume
        16. Cleanup

    """
    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        """
        setUp method
        """
        # Setup_Volume
        self.get_super_method(self, 'setUp')()
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts,
                                                 volume_create_force=True)
        if not ret:
            raise ExecutionError("Failed to setup and mount volume")
        g.log.info("Volume %s has been setup successfully", self.volname)

    def tearDown(self):
        """
        tearDown
        """
        ret, _, _ = snap_delete_all(self.mnode)
        if not ret:
            raise ExecutionError("Snapshot delete failed.")

        # Unmount and cleanup-volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        self.get_super_method(self, 'tearDown')()

    def test_validate_snaps_restore(self):
        # pylint: disable=too-many-statements
        # Start IO on all mounts.
        all_mounts_procs = []
        count = 1
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        # Validate IO
        g.log.info("Validating IO's")
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all io's")

        # Get stat of all the files/dirs created.
        g.log.info("Get stat of all the files/dirs created.")
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfully got stat of all files/dirs created")

        # Setting some volume option related to snapshot
        option_before_restore = {'volumeConfig':
                                 [{'softLimit': '100',
                                   'effectiveHardLimit': '200',
                                   'hardLimit': '256'}],
                                 'systemConfig':
                                 {'softLimit': '90%',
                                  'activateOnCreate': 'disable',
                                  'hardLimit': '256',
                                  'autoDelete': 'disable'}}
        ret = set_snap_config(self.mnode, option_before_restore)
        self.assertTrue(ret, ("Failed to set vol option on  %s"
                              % self.volname))
        g.log.info("Volume options for%s is set successfully", self.volname)

        # Get brick list before taking snap_restore
        bricks_before_snap_restore = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List before snap restore "
                   "volume: %s", bricks_before_snap_restore)

        # Creating snapshot
        ret = snap_create(self.mnode, self.volname, "snap1")
        self.assertTrue(ret, ("Failed to create snapshot for %s"
                              % self.volname))
        g.log.info("Snapshot snap1 created successfully for volume  %s",
                   self.volname)

        # Again start IO on all mounts.
        all_mounts_procs = []
        count = 1000
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        # Validate IO
        g.log.info("Validating IO's")
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all io's")

        # Get stat of all the files/dirs created.
        g.log.info("Get stat of all the files/dirs created.")
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfully got stat of all files/dirs created")

        # Reset volume to make sure volume options will reset
        ret = volume_reset(self.mnode, self.volname, force=False)
        self.assertTrue(ret, ("Failed to reset %s" % self.volname))
        g.log.info("Reset Volume %s is Successful", self.volname)

        # Removing one brick
        g.log.info("Starting volume shrink")
        ret = shrink_volume(self.mnode, self.volname, force=True)
        self.assertTrue(ret, ("Failed to shrink the volume on "
                              "volume %s", self.volname))
        g.log.info("Shrinking volume is successful on "
                   "volume %s", self.volname)

        # Restore snapshot
        ret = snap_restore_complete(self.mnode, self.volname, "snap1")
        self.assertTrue(ret, ("Failed to restore snap snap1 on the "
                              "volume %s", self.volname))
        g.log.info("Restore of volume is successful from snap1 on "
                   "volume  %s", self.volname)

        # Validate volume is up and running
        g.log.info("Verifying volume is up and process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online",
                              self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Get volume options post restore
        option_after_restore = get_snap_config(self.mnode)
        # Compare volume options
        self.assertNotEqual(option_before_restore, option_after_restore,
                            "Volume Options are not same after snap restore")

        # Get brick list post restore
        bricks_after_snap_restore = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List after snap restore "
                   "volume: %s", bricks_after_snap_restore)
        # Compare brick_list
        self.assertNotEqual(bricks_before_snap_restore,
                            bricks_after_snap_restore,
                            "Bricks are not same after snap restore")

        # Creating snapshot
        ret = snap_create(self.mnode, self.volname, "snap2")
        self.assertTrue(ret, ("Failed to create snapshot for %s"
                              % self.volname))
        g.log.info("Snapshot snap2 created successfully for volume  %s",
                   self.volname)

        # Again start IO on all mounts after restore
        all_mounts_procs = []
        count = 1000
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        # Validate IO
        g.log.info("Validating IO's")
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all io's")

        # Get stat of all the files/dirs created.
        g.log.info("Get stat of all the files/dirs created.")
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfully got stat of all files/dirs created")
