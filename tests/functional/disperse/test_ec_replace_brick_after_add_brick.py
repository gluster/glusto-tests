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

from random import choice

from glusto.core import Glusto as g
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.glusterfile import (remove_file,
                                            occurences_of_pattern_in_file)
from glustolibs.gluster.heal_libs import monitor_heal_completion
from glustolibs.gluster.volume_libs import (replace_brick_from_volume,
                                            expand_volume)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (validate_io_procs, wait_for_io_to_complete)


@runs_on([['dispersed'], ['glusterfs']])
class TestEcReplaceBrickAfterAddBrick(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload IO scripts for running IO on mounts
        cls.script_upload_path = (
            "/usr/share/glustolibs/io/scripts/file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients {}".
                                 format(cls.clients))

    @classmethod
    def tearDownClass(cls):
        for each_client in cls.clients:
            ret = remove_file(each_client, cls.script_upload_path)
            if not ret:
                raise ExecutionError("Failed to delete file {}".
                                     format(cls.script_upload_path))
        cls.get_super_method(cls, 'tearDownClass')()

    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup volume and mount it on three clients.
        if not self.setup_volume_and_mount_volume(self.mounts):
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")

    def tearDown(self):
        if self.all_mounts_procs:
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if ret:
                raise ExecutionError(
                    "Wait for IO completion failed on some of the clients")

        # Unmount and cleanup the volume
        if not self.unmount_volume_and_cleanup_volume(self.mounts):
            raise ExecutionError("Unable to unmount and cleanup volume")

        self.get_super_method(self, 'tearDown')()

    def test_ec_replace_brick_after_add_brick(self):
        """
        Test Steps:
        1. Create a pure-ec volume (say 1x(4+2))
        2. Mount volume on two clients
        3. Create some files and dirs from both mnts
        4. Add bricks in this case the (4+2) ie 6 bricks
        5. Create a new dir(common_dir) and in that directory create a distinct
           directory(using hostname as dirname) for each client and pump IOs
           from the clients(dd)
        6. While IOs are in progress replace any of the bricks
        7. Check for errors if any collected after step 6
        """
        # pylint: disable=unsubscriptable-object,too-many-locals
        all_bricks = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(all_bricks, "Unable to get the bricks from the {}"
                                         " volume".format(self.volname))

        self.all_mounts_procs = []
        for count, mount_obj in enumerate(self.mounts):
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d --dir-depth 3 --dir-length 5 "
                   "--max-num-of-dirs 5 --num-of-files 5 %s" % (
                       self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)

        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on the mounts")
        self.all_mounts_procs *= 0

        # Expand the volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, "Expanding volume failed")

        # Create a new dir(common_dir) on mountpoint
        common_dir = self.mounts[0].mountpoint + "/common_dir"
        ret = mkdir(self.mounts[0].client_system, common_dir)
        self.assertTrue(ret, "Directory creation failed")

        # Create distinct directory for each client under common_dir
        distinct_dir = common_dir + "/$HOSTNAME"
        for each_client in self.clients:
            ret = mkdir(each_client, distinct_dir)
            self.assertTrue(ret, "Directory creation failed")

        # Run dd in the background and stdout,stderr to error.txt for
        # validating any errors after io completion.
        run_dd_cmd = ("cd {}; for i in `seq 1 1000`; do dd if=/dev/urandom "
                      "of=file$i bs=4096 count=10 &>> error.txt; done".
                      format(distinct_dir))
        for each_client in self.clients:
            proc = g.run_async(each_client, run_dd_cmd)
            self.all_mounts_procs.append(proc)

        # Get random brick from the bricks
        brick_to_replace = choice(all_bricks)
        node_from_brick_replace, _ = brick_to_replace.split(":")

        # Replace brick from the same node
        servers_info_of_replaced_node = {}
        servers_info_of_replaced_node[node_from_brick_replace] = (
            self.all_servers_info[node_from_brick_replace])

        ret = replace_brick_from_volume(self.mnode, self.volname,
                                        node_from_brick_replace,
                                        servers_info_of_replaced_node,
                                        src_brick=brick_to_replace)
        self.assertTrue(ret, "Replace brick failed")

        self.assertTrue(validate_io_procs(self.all_mounts_procs, self.mounts),
                        "IO failed on the mounts")
        self.all_mounts_procs *= 0

        err_msg = "Too many levels of symbolic links"
        dd_log_file = distinct_dir + "/error.txt"
        for each_client in self.clients:
            ret = occurences_of_pattern_in_file(each_client, err_msg,
                                                dd_log_file)
            self.assertEqual(ret, 0, "Either file {} doesn't exist or {} "
                             "messages seen while replace brick operation "
                             "in-progress".format(dd_log_file, err_msg))

        self.assertTrue(monitor_heal_completion(self.mnode, self.volname),
                        "Heal failed on the volume {}".format(self.volname))
