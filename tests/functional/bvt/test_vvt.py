#  Copyright (C) 2015-2016  Red Hat, Inc. <http://www.redhat.com>
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
    Description: BVT-Volume Verification Tests (VVT). Tests the Basic
    Volume Operations like start, status, stop, delete.

"""

import pytest
import time
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterVolumeBaseClass,
                                                   runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_init import is_glusterd_running
from glustolibs.gluster.volume_ops import volume_stop, volume_start
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online)
from glustolibs.gluster.volume_libs import log_volume_info_and_status
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs, get_mounts_stat


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class VolumeAccessibilityTests(GlusterVolumeBaseClass):
    """ VolumeAccessibilityTests contains tests which verifies
        accessablity of the volume.
    """
    @classmethod
    def setUpClass(cls):
        """Setup Volume, Create Mounts and upload the necessary scripts to run
        tests.
        """
        # Sets up volume, mounts
        GlusterVolumeBaseClass.setUpClass.im_func(cls)

        # Upload io scripts for running IO on mounts
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, script_local_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts")

    @pytest.mark.bvt_vvt
    def test_volume_create_start_stop_start(self):
        """Tests volume create, start, status, stop, start.
        Also Validates whether all the brick process are running after the
        start of the volume.
        """
        # Verify volume's all process are online
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online" %
                              self.volname))

        # Stop Volume
        ret, _, _ = volume_stop(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, "Failed to stop volume %s" % self.volname)

        # Start Volume
        ret, _, _ = volume_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Unable to start volume %s" % self.volname)

        time.sleep(15)

        # Log Volume Info and Status
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume %s info and status failed" %
                              self.volname))

        # Verify volume's all process are online
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online" %
                              self.volname))

        # Log Volume Info and Status
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume %s info and status failed" %
                              self.volname))

        # Verify all glusterd's are running
        ret = is_glusterd_running(self.servers)
        self.assertEqual(ret, 0, ("glusterd not running on all servers: %s" %
                                  self.servers))

    @pytest.mark.bvt_vvt
    def test_file_dir_create_ops_on_volume(self):
        """Test File Directory Creation on the volume.
        """
        # Start IO on all mounts.
        all_mounts_procs = []
        count = 1
        for mount_obj in self.mounts:
            cmd = ("python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (self.script_upload_path, count,
                                            mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        # Validate IO
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")

        # Get stat of all the files/dirs created.
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
