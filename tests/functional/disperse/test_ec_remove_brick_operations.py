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

"""
Description:

    This test verifies remove brick operation on EC
    volume.

"""
from time import sleep
from glusto.core import Glusto as g
from glustolibs.io.utils import (validate_io_procs,
                                 wait_for_io_to_complete)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.brick_ops import (remove_brick)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import (get_subvols,
                                            get_volume_info,
                                            log_volume_info_and_status)
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.rebalance_ops import (
    wait_for_remove_brick_to_complete)


@runs_on([['distributed-dispersed'], ['glusterfs']])
class EcRemoveBrickOperations(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Ensure we have sufficient subvols
        self.volume['voltype']['dist_count'] = 4

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        # Start IO on mounts
        self.counter = 1
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 5 "
                   "--max-num-of-dirs 3 "
                   "--num-of-files 3 %s" % (self.script_upload_path,
                                            self.counter,
                                            mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            self.counter += 10
        self.io_validation_complete = False

        # Adding a delay of 10 seconds before test method starts. This
        # is to ensure IO's are in progress and giving some time to fill data
        sleep(10)

    def test_remove_brick_operations(self):
        """
        Steps:
        1. Remove data brick count number of bricks from the volume
           should fail
        2. step 1 with force option should fail
        3. Remove redundant brick count number of bricks from the volume
           should fail
        4. step 3 with force option should fail
        5. Remove data brick count+1 number of bricks from the volume
           should fail
        6. step 5 with force option should fail
        7. Remove disperse count number of bricks from the volume with
           one wrong brick path should fail
        8. step 7 with force option should fail
        9. Start remove brick on first subvol bricks
        10. Remove all the subvols to make a pure EC vol
            by start remove brick on second subvol bricks
        11. Start remove brick on third subvol bricks
        12. Write files and perform read on mountpoints
        """
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-statements

        subvols_list = get_subvols(self.mnode, self.volname)
        volinfo = get_volume_info(self.mnode, self.volname)
        initial_brickcount = volinfo[self.volname]['brickCount']
        data_brick_count = (self.volume['voltype']['disperse_count'] -
                            self.volume['voltype']['redundancy_count'])

        # Try to remove data brick count number of bricks from the volume
        bricks_list_to_remove = (subvols_list['volume_subvols'][0]
                                 [0:data_brick_count])
        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 bricks_list_to_remove,
                                 option="start")
        self.assertEqual(
            ret, 1, ("ERROR: Removed bricks %s from the volume "
                     "%s" % (bricks_list_to_remove, self.volname)))

        # Trying with force option
        ret, _, _ = remove_brick(
            self.mnode, self.volname, bricks_list_to_remove, option="force")
        self.assertEqual(
            ret, 1, ("ERROR: Removed bricks %s from the volume "
                     "%s" % (bricks_list_to_remove, self.volname)))

        # Try to remove redundant brick count number of bricks from the volume
        bricks_list_to_remove = (subvols_list['volume_subvols'][0]
                                 [0:self.volume['voltype']
                                  ['redundancy_count']])
        ret, _, _ = remove_brick(
            self.mnode, self.volname, bricks_list_to_remove, option="start")
        self.assertEqual(
            ret, 1, ("ERROR: Removed bricks %s from the volume "
                     "%s" % (bricks_list_to_remove, self.volname)))

        # Trying with force option
        ret, _, _ = remove_brick(
            self.mnode, self.volname, bricks_list_to_remove, option="force")
        self.assertEqual(
            ret, 1, ("ERROR: Removed bricks %s from the volume"
                     "%s" % (bricks_list_to_remove, self.volname)))

        # Try to remove data brick count+1 number of bricks from the volume
        bricks_list_to_remove = (subvols_list['volume_subvols'][0]
                                 [0:data_brick_count + 1])
        ret, _, _ = remove_brick(
            self.mnode, self.volname, bricks_list_to_remove, option="start")
        self.assertEqual(
            ret, 1, ("ERROR: Removed bricks %s from the volume "
                     "%s" % (bricks_list_to_remove, self.volname)))

        # Trying with force option
        ret, _, _ = remove_brick(
            self.mnode, self.volname, bricks_list_to_remove, option="force")
        self.assertEqual(
            ret, 1, ("ERROR: Removed bricks %s from the volume "
                     "%s" % (bricks_list_to_remove, self.volname)))

        # Try to remove disperse count number of bricks from the volume with
        # one wrong brick path
        bricks_list_to_remove = (subvols_list['volume_subvols'][0]
                                 [0:self.volume['voltype']['disperse_count']])
        bricks_list_to_remove[0] = bricks_list_to_remove[0] + "wrong_path"
        ret, _, _ = remove_brick(
            self.mnode, self.volname, bricks_list_to_remove, option="start")
        self.assertEqual(
            ret, 1, ("ERROR: Removed bricks %s from the volume "
                     "%s" % (bricks_list_to_remove, self.volname)))

        # Trying with force option
        ret, _, _ = remove_brick(
            self.mnode, self.volname, bricks_list_to_remove, option="force")
        self.assertEqual(
            ret, 1, ("ERROR: Removed bricks %s from the volume "
                     "%s" % (bricks_list_to_remove, self.volname)))

        # Verify that the brick count is intact
        volinfo = get_volume_info(self.mnode, self.volname)
        latest_brickcount = volinfo[self.volname]['brickCount']
        self.assertEqual(initial_brickcount, latest_brickcount,
                         ("Brick count is not expected to "
                          "change, but changed"))

        # Start remove brick on first subvol bricks
        bricks_list_to_remove = subvols_list['volume_subvols'][0]
        ret, _, _ = remove_brick(
            self.mnode, self.volname, bricks_list_to_remove, option="start")
        self.assertEqual(
            ret, 0, ("Failed to remove bricks %s from the volume "
                     "%s" % (bricks_list_to_remove, self.volname)))

        # Verify that the brick count is intact
        volinfo = get_volume_info(self.mnode, self.volname)
        latest_brickcount = volinfo[self.volname]['brickCount']
        self.assertEqual(initial_brickcount, latest_brickcount,
                         ("Brick count is not expected to "
                          "change, but changed"))

        # Wait for remove brick to complete
        ret = wait_for_remove_brick_to_complete(self.mnode, self.volname,
                                                bricks_list_to_remove)
        self.assertTrue(
            ret, ("Remove brick is not yet complete on the volume "
                  "%s" % self.volname))
        g.log.info("Remove brick is successfully complete on the volume %s",
                   self.volname)

        # Commit the remove brick operation
        ret, _, _ = remove_brick(
            self.mnode, self.volname, bricks_list_to_remove, option="commit")
        self.assertEqual(
            ret, 0, ("Failed to commit remove bricks %s from the volume "
                     "%s" % (bricks_list_to_remove, self.volname)))

        # Remove all the subvols to make a pure EC vol
        # Start remove brick on second subvol bricks
        bricks_list_to_remove = subvols_list['volume_subvols'][1]
        ret, _, _ = remove_brick(
            self.mnode, self.volname, bricks_list_to_remove, option="start")
        self.assertEqual(
            ret, 0, ("Failed to remove bricks %s from the volume "
                     "%s" % (bricks_list_to_remove, self.volname)))

        # Wait for remove brick to complete
        ret = wait_for_remove_brick_to_complete(self.mnode, self.volname,
                                                bricks_list_to_remove)
        self.assertTrue(
            ret, ("Remove brick is not yet complete on the volume "
                  "%s", self.volname))

        # Commit the remove brick operation
        ret, _, _ = remove_brick(
            self.mnode, self.volname, bricks_list_to_remove, option="commit")
        self.assertEqual(
            ret, 0, ("Failed to commit remove bricks %s from the volume"
                     " %s" % (bricks_list_to_remove, self.volname)))
        g.log.info("Remove brick is successfully complete on the volume %s",
                   self.volname)

        # Start remove brick on third subvol bricks
        bricks_list_to_remove = subvols_list['volume_subvols'][2]
        ret, _, _ = remove_brick(
            self.mnode, self.volname, bricks_list_to_remove, option="start")
        self.assertEqual(ret, 0, ("Failed to remove bricks %s from "
                                  "the volume %s" % (
                                      bricks_list_to_remove, self.volname)))

        # Wait for remove brick to complete
        ret = wait_for_remove_brick_to_complete(self.mnode, self.volname,
                                                bricks_list_to_remove)
        self.assertTrue(
            ret, ("Remove brick is not yet complete on the volume "
                  "%s" % self.volname))
        g.log.info("Remove brick is successfully complete on the volume %s",
                   self.volname)

        # Commit the remove brick operation
        ret, _, _ = remove_brick(
            self.mnode, self.volname, bricks_list_to_remove, option="commit")
        self.assertEqual(
            ret, 0, ("Failed to commit remove bricks %s from the volume "
                     "%s" % (bricks_list_to_remove, self.volname)))
        g.log.info("Remove brick is successfully complete on the volume %s",
                   self.volname)

        # Log volume info and status
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed "
                              "on volume %s" % self.volname))
        g.log.info("Successful in logging volume info and status "
                   "of volume %s", self.volname)

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Write some files on the mount point
        cmd1 = ("cd %s; mkdir test; cd test; for i in `seq 1 100` ;"
                "do touch file$i; done" % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd1)
        self.assertEqual(ret, 0, ("Write operation failed on client "
                                  "%s " % self.mounts[0].client_system))
        g.log.info("Writes on mount point successful")

        # Perform read operation on mountpoint
        cmd2 = ("cd %s; ls -lRt;" % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd2)
        self.assertEqual(ret, 0, ("Read operation failed on client "
                                  "%s " % self.mounts[0].client_system))
        g.log.info("Read on mount point successful")

    def tearDown(self):
        # Wait for IO to complete if io validation is not executed in the
        # test method
        if not self.io_validation_complete:
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")
            g.log.info("IO is successful on all mounts")

        # Stopping the volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
