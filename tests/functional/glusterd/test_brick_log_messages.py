#  Copyright (C) 2018  Red Hat, Inc. <http://www.redhat.com>
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
        No Errors should generate in brick logs after deleting files
        from mountpoint
"""


from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.mount_ops import is_mounted


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestAddBrickFunctionality(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.counter = 1
        GlusterBaseClass.setUpClass.im_func(cls)

        # Uploading file_dir script in all client direcotries
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, script_local_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        """
        setUp method for every test
        """
        # calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        # Creating Volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Volume creation or mount failed for %s"
                                 % self.volname)
        g.log.info("Volme created and mounted successfully : %s",
                   self.volname)

    def tearDown(self):
        """
        tearDown for every test
        """

        # stopping the volume and Cleaning up the volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed Cleanup the Volume %s" % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_brick_log_messages(self):
        '''
        -> Create volume
        -> Mount volume
        -> write files on mount point
        -> delete files from mount point
        -> check for any errors filled in all brick logs
        '''

        # checking volume mounted or not
        for mount_obj in self.mounts:
            ret = is_mounted(self.volname, mount_obj.mountpoint, self.mnode,
                             mount_obj.client_system, self.mount_type)
            self.assertTrue(ret, "Not mounted on %s"
                            % mount_obj.client_system)
            g.log.info("Mounted on %s", mount_obj.client_system)

        # run IOs
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 5 "
                   "--max-num-of-dirs 3 "
                   "--num-of-files 10 %s" % (self.script_upload_path,
                                             self.counter,
                                             mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            self.counter = self.counter + 10

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # Getting timestamp
        _, timestamp, _ = g.run_local('date +%s')
        timestamp = timestamp.strip()

        # Getting all bricks
        brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(brick_list, "Failed to get brick list")
        g.log.info("Successful in getting brick list %s", brick_list)

        # Creating dictionary for each node brick path,
        # here nodes are keys and brick paths are values
        brick_path_dict = {}
        for brick in brick_list:
            node, brick_path = brick.split(r':')
            brick_path_list = brick_path.split(r'/')
            del brick_path_list[0]
            brick_log_path = '-'.join(brick_path_list)
            brick_path_dict[node] = brick_log_path

        for node in brick_path_dict:
            #  Copying brick logs into other file for backup purpose
            ret, _, _ = g.run(node, 'cp /var/log/glusterfs/bricks/%s.log '
                                    '/var/log/glusterfs/bricks/%s_%s.log'
                              % (brick_path_dict[node], brick_path_dict[node],
                                 timestamp))
            if ret:
                raise ExecutionError("Failed to copy brick logs of %s" % node)
            g.log.info("Brick logs copied successfully on node %s", node)

            # Clearing the existing brick log file
            ret, _, _ = g.run(node, 'echo > /var/log/glusterfs/bricks/%s.log'
                              % brick_path_dict[node])
            if ret:
                raise ExecutionError("Failed to clear brick log file on %s"
                                     % node)
            g.log.info("Successfully cleared the brick log files on node %s",
                       node)

        # Deleting files from mount point
        ret, _, _ = g.run(self.mounts[0].client_system, 'rm -rf %s/*'
                          % self.mounts[0].mountpoint)
        self.assertEqual(ret, 0, "Failed to delete files from mountpoint %s"
                         % self.mounts[0].mountpoint)
        g.log.info("Files deleted successfully from mountpoint %s",
                   self.mounts[0].mountpoint)

        # Searching for error messages in brick logs after deleting
        # files from mountpoint
        for node in brick_path_dict:
            ret, out, _ = g.run(
                node, "grep ' E ' /var/log/glusterfs/bricks/%s.log | wc -l" %
                brick_path_dict[node])
            self.assertEqual(int(out), 0, "Found Error messages in brick "
                                          "log %s" % node)
            g.log.info("No error messages found in brick log %s", node)
