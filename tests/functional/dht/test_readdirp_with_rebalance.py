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

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.rebalance_ops import (set_rebalance_throttle,
                                              rebalance_start,
                                              get_rebalance_status)
from glustolibs.gluster.volume_libs import form_bricks_list_to_add_brick
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs


@runs_on([['distributed', 'replicated', 'dispersed',
           'arbiter', 'distributed-dispersed',
           'distributed-replicated', 'distributed-arbiter'],
          ['glusterfs']])
class TestReaddirpWithRebalance(GlusterBaseClass):
    def setUp(self):
        """
        Setup and mount volume or raise ExecutionError
        """
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()
        self.all_mounts_procs, self.io_validation_complete = [], False

        # Setup Volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to Setup and Mount Volume")

        # Upload io scripts for running IO on mounts
        self.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                   "file_dir_ops.py")
        ret = upload_scripts(self.clients[0], self.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients")

        # Form brick list for expanding volume
        self.add_brick_list = form_bricks_list_to_add_brick(
            self.mnode, self.volname, self.servers, self.all_servers_info,
            distribute_count=3)
        if not self.add_brick_list:
            raise ExecutionError("Volume %s: Failed to form bricks list for"
                                 " expand" % self.volname)
        g.log.info("Volume %s: Formed bricks list for expand", self.volname)

    def tearDown(self):
        # Unmount and cleanup original volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_readdirp_with_rebalance(self):
        """
        Description: Tests to check that all directories are read
                     and listed while rebalance is still in progress.

        Steps :
        1) Create a volume.
        2) Mount the volume using FUSE.
        3) Create a dir "master" on mount-point.
        4) Create 8000 empty dirs (dir1 to dir8000) inside dir "master".
        5) Now inside a few dirs (e.g. dir1 to dir10), create deep dirs
           and inside every dir, create 50 files.
        6) Collect the number of dirs present on /mnt/<volname>/master
        7) Change the rebalance throttle to lazy.
        8) Add-brick to the volume (at least 3 replica sets.)
        9) Start rebalance using "force" option on the volume.
        10) List the directories on dir "master".
        """
        # pylint: disable=too-many-statements
        # Start IO on mounts
        m_point = self.mounts[0].mountpoint
        ret = mkdir(self.mounts[0].client_system,
                    "{}/master".format(m_point))
        self.assertTrue(ret, "mkdir of dir master failed")

        # Create 8000 empty dirs
        cmd = ("ulimit -n 64000; /usr/bin/env python {} create_deep_dir"
               " --dir-length 8000 --dir-depth 0"
               " {}/master/".format(self.script_upload_path, m_point))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)
        g.log.info("IO on %s:%s is started successfully",
                   self.mounts[0].client_system, m_point)

        # Validate 8000 empty dirs are created successfully
        ret = validate_io_procs(self.all_mounts_procs, self.mounts[0])
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Create deep dirs and files
        self.all_mounts_procs = []
        cmd = ("/usr/bin/env python {} create_deep_dirs_with_files"
               " --dir-length 10 --dir-depth 1 --max-num-of-dirs 50 "
               " --num-of-files 50 --file-type empty-file"
               " {}/master/".format(self.script_upload_path, m_point))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)
        g.log.info("IO on %s:%s is started successfully",
                   self.mounts[0].client_system, m_point)

        # Validate deep dirs and files are created successfully
        ret = validate_io_procs(self.all_mounts_procs, self.mounts[0])
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Check the dir count before rebalance
        cmd = ('cd {}/master; ls -l | wc -l'.format(m_point))
        ret, dir_count_before, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Failed to "
                         "get directory count")
        g.log.info("Dir count before %s", dir_count_before)

        # Change the rebalance throttle to lazy
        ret, _, _ = set_rebalance_throttle(self.mnode, self.volname,
                                           throttle_type='lazy')
        self.assertEqual(ret, 0, "Failed to set rebal-throttle to lazy")
        g.log.info("Rebal-throttle set to 'lazy' successfully")

        # Add-bricks to the volume
        ret, _, _ = add_brick(self.mnode, self.volname, self.add_brick_list)
        self.assertEqual(ret, 0, "Failed to add-brick to the volume")
        g.log.info("Added bricks to the volume successfully")

        # Start rebalance using force
        ret, _, _ = rebalance_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, "Failed to start rebalance")
        g.log.info("Rebalance started successfully")

        # Check if rebalance is in progress
        rebalance_status = get_rebalance_status(self.mnode, self.volname)
        status = rebalance_status['aggregate']['statusStr']
        self.assertEqual(status, "in progress",
                         ("Rebalance is not in 'in progress' state,"
                          " either rebalance is in compeleted state"
                          " or failed to get rebalance status"))

        # Check the dir count after rebalance
        cmd = ('cd {}/master; ls -l | wc -l'.format(m_point))
        ret, dir_count_after, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Failed to do lookup and"
                         " get directory count")
        g.log.info("Dir count after %s", dir_count_after)

        # Check if there is any data loss
        self.assertEqual(set(dir_count_before), set(dir_count_after),
                         ("There is data loss"))
        g.log.info("The checksum before and after rebalance is same."
                   " There is no data loss.")
