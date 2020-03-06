#  Copyright (C) 2017-2020  Red Hat, Inc. <http://www.redhat.com>
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

#  Description:
#        Test Cases in this module related to Glusterd network ping timeout
#        of the volume.

import re

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (wait_for_io_to_complete,
                                 list_all_files_and_dirs_mounts)
from glustolibs.gluster.volume_ops import (get_volume_options,
                                           set_volume_options)
from glustolibs.gluster.mount_ops import is_mounted
from glustolibs.io.utils import collect_mounts_arequal


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'], ['glusterfs']])
class CheckVolumeChecksumAfterChangingNetworkPingTimeOut(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()
        g.log.info("Starting %s ", cls.__name__)

        # Uploading file_dir script in all client direcotries
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
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
        self.get_super_method(self, 'setUp')()

        # Creating Volume
        g.log.info("Started creating volume")
        ret = self.setup_volume()
        if ret:
            g.log.info("Volme created successfully : %s", self.volname)
        else:
            raise ExecutionError("Volume creation failed: %s" % self.volname)

    def tearDown(self):
        """
        tearDown for every test
        """
        # unmounting the volume and Cleaning up the volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if ret:
            g.log.info("Volume deleted successfully : %s", self.volname)
        else:
            raise ExecutionError("Failed Cleanup the Volume %s" % self.volname)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_volume_checksum_after_changing_network_ping_timeout(self):

        # Create Volume
        # Mount the Volume
        # Create some files on mount point
        # calculate the checksum of Mount point
        # Check the default network ping timeout of the volume.
        # Change network ping timeout to some other value
        # calculate checksum again
        # checksum should be same without remounting the volume.

        # Mounting volume as glusterfs
        ret = self.mount_volume(self.mounts)
        self.assertTrue(ret, "volume mount failed for %s" % self.volname)
        g.log.info("Volume mounted successfully : %s", self.volname)

        # Checking volume mounted or not
        ret = is_mounted(self.volname, self.mounts[0].mountpoint, self.mnode,
                         self.mounts[0].client_system, self.mount_type)
        self.assertTrue(ret, "Volume not mounted on mount point: %s"
                        % self.mounts[0].mountpoint)
        g.log.info("Volume %s mounted on %s", self.volname,
                   self.mounts[0].mountpoint)

        # run IOs
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_files -f 10 "
                   "--base-file-name newfile %s" % (
                       self.script_upload_path,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Checksum calculation of mount point before
        # changing network.ping-timeout
        ret, before_checksum = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, "checksum failed to calculate for mount point")
        g.log.info("checksum calculated successfully")

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")

        # performing gluster volume get volname all and
        # getting network ping time out value
        volume_options = get_volume_options(self.mnode, self.volname, "all")
        self.assertIsNotNone(volume_options, "gluster volume get %s all "
                                             "command failed" % self.volname)
        g.log.info("gluster volume get %s all command executed "
                   "successfully", self.volname)
        ret = False
        if re.search(r'\b42\b', volume_options['network.ping-timeout']):
            ret = True
        self.assertTrue(ret, "network ping time out value is not correct")
        g.log.info("network ping time out value is correct")

        # Changing network ping time out value to specific volume
        self.networking_ops = {'network.ping-timeout': '12'}
        ret = set_volume_options(self.mnode, self.volname,
                                 self.networking_ops)
        self.assertTrue(ret, "Changing of network.ping-timeout "
                             "failed for :%s" % self.volname)
        g.log.info("Changing of network.ping-timeout "
                   "success for :%s", self.volname)

        # Checksum calculation of mount point after
        # changing network.ping-timeout
        ret, after_checksum = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, "checksum failed to calculate for mount point")
        g.log.info("checksum calculated successfully")

        # comparing list of checksums of mountpoints before and after
        # network.ping-timeout change
        self.assertItemsEqual(before_checksum, after_checksum,
                              "Checksum not same before and after "
                              "network.ping-timeout change")
        g.log.info("checksum same before and after "
                   "changing network.ping-timeout")

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")
