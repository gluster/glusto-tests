#  Copyright (C) 2018-2019  Red Hat, Inc. <http://www.redhat.com>
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
from copy import deepcopy
import sys

from glusto.core import Glusto as g

from glustolibs.gluster.nfs_ganesha_libs import NfsGaneshaClusterSetupClass
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs, get_mounts_stat


@runs_on([['replicated', 'distributed', 'distributed-replicated'],
          ['nfs']])
class TestMountWhileIoInProgress(NfsGaneshaClusterSetupClass):
    """
    Test cases to validate new mount while IO is going on
    """

    @classmethod
    def setUpClass(cls):
        """
        Setup nfs-ganesha if not exists.
        Upload IO scripts to clients
        """
        cls.get_super_method(cls, 'setUpClass')()

        # Setup nfs-ganesha if not exists.
        ret = cls.setup_nfs_ganesha()
        if not ret:
            raise ExecutionError("Failed to setup nfs-ganesha cluster")
        g.log.info("nfs-ganesha cluster is healthy")

        # Upload IO scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, script_local_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        """
        Setup volume
        """
        g.log.info("Starting to setup and mount volume %s", self.volname)
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Successfully setup volume %s", self.volname)

    def test_new_mount_while_io_in_progress(self):
        """
        Verify new mount will not cause any issues while IO is running
        Steps:
        1. Mount volume on one client
        2. Start IO
        3. Mount volume on new mountpoint
        4. Start IO on new mountpoint
        5. Validate IOs
        """
        # Take 2 mounts if available
        no_of_mount_objects = len(self.mounts)
        if no_of_mount_objects == 1:
            self.mount_obj1 = self.mounts[0]
            self.mount_obj2 = deepcopy(self.mounts[0])
            self.mount_obj2.mountpoint = '%s_new' % self.mount_obj2.mountpoint
        else:
            self.mount_obj1 = self.mounts[0]
            self.mount_obj2 = self.mounts[1]

        self.new_mounts = [self.mount_obj1, self.mount_obj2]

        all_mounts_procs = []
        dirname_start_num = 1

        for mount_object in self.new_mounts:
            # Mount volume
            ret = self.mount_obj1.mount()
            self.assertTrue(ret, "Unable to mount volume %s on %s"
                            % (mount_object.volname,
                               mount_object.client_system))

            # Start IO
            g.log.info("Starting IO on %s:%s", mount_object.client_system,
                       mount_object.mountpoint)
            cmd = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       sys.version_info.major, self.script_upload_path,
                       dirname_start_num, mount_object.mountpoint))
            proc = g.run_async(mount_object.client_system, cmd,
                               user=mount_object.user)
            all_mounts_procs.append(proc)
            dirname_start_num += 10

        # Validate IOs
        g.log.info("Validating IO's")
        ret = validate_io_procs(all_mounts_procs, self.new_mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all IOs")

        # Get stat of all the files/dirs created.
        g.log.info("Get stat of all the files/dirs created.")
        ret = get_mounts_stat(self.new_mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfully got stat of all files/dirs created")

    def tearDown(self):
        """
        Unmount and cleanup volume
        """
        # Unmount volume
        ret = self.unmount_volume(self.new_mounts)
        if ret:
            g.log.info("Successfully unmounted the volume")
        else:
            g.log.error("Failed to unmount volume")

        # Cleanup volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup volume")
        g.log.info("Cleanup volume %s completed successfully", self.volname)

    @classmethod
    def tearDownClass(cls):
        cls.get_super_method(cls, 'tearDownClass')(
            delete_nfs_ganesha_cluster=False)
