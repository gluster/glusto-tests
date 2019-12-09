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

import sys

from glusto.core import Glusto as g

from glustolibs.gluster.nfs_ganesha_libs import NfsGaneshaClusterSetupClass
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs, get_mounts_stat
from glustolibs.gluster.volume_libs import (
    log_volume_info_and_status,
    replace_brick_from_volume,
    wait_for_volume_process_to_be_online)
from glustolibs.gluster.heal_libs import monitor_heal_completion


@runs_on([['distributed-replicated', 'replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestGaneshaReplaceBrick(NfsGaneshaClusterSetupClass):
    """
    Test cases to validate remove brick functionality on volumes
    exported through nfs-ganesha
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
        Setup and mount volume
        """
        g.log.info("Starting to setup and mount volume %s", self.volname)
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup and mount volume %s"
                                 % self.volname)
        g.log.info("Successful in setup and mount volume %s", self.volname)

    def test_nfs_ganesha_replace_brick(self):
        """
        Verify replace brick operation while IO is running
        Steps:
        1. Start IO on mount points
        2. Perofrm replace brick operation
        3. Validate IOs
        4. Get stat of files and dris
        """
        # pylint: disable=too-many-statements
        # Start IO on all mount points
        all_mounts_procs = []
        count = 1
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       sys.version_info.major, self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        # Get stat of all the files/dirs created.
        g.log.info("Get stat of all the files/dirs created.")
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfully got stat of all files/dirs created")

        # Perform replace brick operation
        g.log.info("Replace a brick from the volume")
        ret = replace_brick_from_volume(self.mnode, self.volname,
                                        self.servers, self.all_servers_info)
        self.assertTrue(ret, "Failed to replace  brick from the volume")
        g.log.info("Replace brick operation successful")

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online after replace "
                   "brick operation")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("All volume %s processes failed to come up "
                              "online", self.volname))
        g.log.info("All volume %s processes came up "
                   "online successfully after replace brick operation",
                   self.volname)

        # Log volume info and status
        g.log.info("Logging volume info and status after replacing brick "
                   "from the volume %s", self.volname)
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to log volume info and status of "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Wait for self-heal to complete
        g.log.info("Wait for self-heal to complete")
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')
        g.log.info("Self-heal is successful after replace-brick operation")

        # Validate IOs
        g.log.info("Validating IO's")
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all io's")

        # Get stat of all the files/dirs created.
        g.log.info("Get stat of all the files/dirs created.")
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfully got stat of all files/dirs created")

    def tearDown(self):
        """
        Unmount and cleanup volume
        """
        # Unmount volume
        ret = self.unmount_volume(self.mounts)
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
