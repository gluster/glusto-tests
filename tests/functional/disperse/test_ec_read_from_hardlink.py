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
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.mount_ops import mount_volume, umount_volume
from glustolibs.gluster.volume_ops import (set_volume_options,
                                           volume_reset)
from glustolibs.gluster.glusterfile import create_link_file


@runs_on([['dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs']])
class TestEcReadFromHardlink(GlusterBaseClass):

    def setUp(self):
        self.get_super_method(self, 'setUp')()

        # Setup volume
        if not self.setup_volume():
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        self.get_super_method(self, 'tearDown')()

        # Unmount the volume
        ret = umount_volume(mclient=self.mounts[0].client_system,
                            mpoint=self.mounts[0].mountpoint)
        if not ret:
            raise ExecutionError("Unable to umount the volume")
        g.log.info("Unmounting of the volume %s succeeded", self.volname)

        # The reason for volume reset is, metadata-cache is enabled
        # by group, can't disable the group in glusterfs.
        ret, _, _ = volume_reset(self.mnode, self.volname)
        if ret:
            raise ExecutionError("Unable to reset the volume {}".
                                 format(self.volname))
        g.log.info("Volume: %s reset successful ", self.volname)

        # Cleanup the volume
        if not self.cleanup_volume():
            raise ExecutionError("Unable to perform volume clenaup")
        g.log.info("Volume cleanup is successfull")

    def test_ec_read_from_hardlink(self):
        """
        Test steps:
        1. Enable metadata-cache(md-cache) options on the volume
        2. Touch a file and create a hardlink for it
        3. Read data from the hardlink.
        4. Read data from the actual file.
        """
        options = {'group': 'metadata-cache'}
        # Set metadata-cache options as group
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, "Unable to set the volume options {}".
                        format(options))
        g.log.info("Able to set the %s options", options)

        # Mounting the volume on one client
        ret, _, _ = mount_volume(self.volname, mtype=self.mount_type,
                                 mpoint=self.mounts[0].mountpoint,
                                 mserver=self.mnode,
                                 mclient=self.mounts[0].client_system)
        self.assertEqual(ret, 0, ("Volume {} is not mounted").
                         format(self.volname))
        g.log.info("Volume mounted successfully : %s", self.volname)

        file_name = self.mounts[0].mountpoint + "/test1"
        content = "testfile"
        hard_link = self.mounts[0].mountpoint + "/test1_hlink"
        cmd = 'echo "{content}" > {file}'.format(file=file_name,
                                                 content=content)

        # Creating a file with data
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Sucessful in creating a file with data")
        g.log.info("file created successfully on %s",
                   self.mounts[0].mountpoint)

        # Creating a hardlink for the file created
        ret = create_link_file(self.mounts[0].client_system,
                               file_name, hard_link)
        self.assertTrue(ret, "Link file creation failed")
        g.log.info("Link file creation for %s is successful", file_name)

        # Reading from the file as well as the hardlink
        for each in (file_name, hard_link):
            ret, out, _ = g.run(self.mounts[0].client_system,
                                "cat {}".format(each))
            self.assertEqual(ret, 0, "Unable to read the {}".format(each))
            self.assertEqual(content, out.strip('\n'), "The content {} and"
                             " data in file {} is not same".
                             format(content, each))
            g.log.info("Read of %s file is successful", each)
