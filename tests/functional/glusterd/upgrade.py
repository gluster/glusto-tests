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

""" Description:
      Verify df output before and after upgrade for all volumes.
"""

import random
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import (are_bricks_online, get_all_bricks)
from glustolibs.misc.misc_libs import (yum_install_packages, upload_scripts)
from glustolibs.io.utils import (wait_for_io_to_complete, collect_mounts_arequal)
from glustolibs.gluster.gluster_init import (start_glusterd, stop_glusterd,
                                             wait_for_glusterd_to_start)


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'arbiter',
           'distributed-arbiter'], ['glusterfs']])
class TestDfBeforeAndAfterUpgrade(GlusterBaseClass):

    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()
        self.all_mounts_procs = []

        # Creating and starting Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume and mount it")
        self.client = self.mounts[0].client_system
        self.mountpoint = self.mounts[0].mountpoint
 
        self.script_upload_path = ("/usr/share/glustolibs/io/scripts/file_dir_ops.py")
        ret = upload_scripts(self.client,self.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to client")
        

    def tearDown(self):
        # Stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup the volume %s"
                                 % self.volname)
        g.log.info("Volume deleted successfully: %s", self.volname)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_df_before_and_after_volume_upgrade(self):
        """
        Test Case:
        1) Create a volume and start it.
        2) mount the volume 
        3) write some data in the volume
        4) Take checksum of that data
        5) Save the df output from the client
        6) Upgrade the cluster node by node
        7) Take checksum of that data again and compare it with previous checksum
        8) Take the df output again and compare it with previous df output 
        9) remove the latest and go back to old version
        """
        # Write some data 
        cmd = ( "/usr/bin/env python %s create_files -f 1024 --fixed-file-size 1M --base-file-name file %s" % (self.script_upload_path,self.mountpoint))
        proc = g.run_async(self.client, cmd, user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)

        # Wait for io to complete # uncomment the below after rest starts working
        self.assertTrue(wait_for_io_to_complete(self.all_mounts_procs, self.mounts[0]), "IO failed on some of the clients")
        g.log.info("IO completed on the clients")

        # compute arequal checksum before upgrade
        arequal_checksum_before_upgrade = collect_mounts_arequal(self.mounts)

        # capture the df output 
        cmd = ("df  %s" % self.mountpoint )
        ret, df_output_before_upgrade, err = g.run(self.client,cmd)
        self.assertFalse(ret, err)

        # wget rpm_location, assuming that rpm_location is having the latest version or wget the nightly master
        # But doing this has a drawback that this will only work for glusto setup having distro as centos, rhel


        #rpm_location = "https://buildlogs.centos.org/centos/8/storage/x86_64/gluster-9/"

        for server in self.servers:
            #cmd = ("wget %s" % rpm_location )
            #ret, _, _ = g.run(server, cmd)
            #self.assertEqual(ret, 0, "failed to get latest repository")
            stop_glusterd(server)
            yum_install_packages(server, 'glusterfs-server')
            start_glusterd(server)

        # compute arequal checksum before upgrade
        arequal_checksum_after_upgrade = collect_mounts_arequal(self.mounts)

        # capture the df output 
        cmd = ("df  %s" % self.mountpoint )
        ret, df_output_after_upgrade, err = g.run(self.client,cmd)
        self.assertFalse(ret, err)
