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

from time import sleep

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.volume_ops import get_volume_info
from glustolibs.gluster.shared_storage_ops import (enable_shared_storage,
                                                   is_shared_volume_mounted,
                                                   disable_shared_storage)
from glustolibs.gluster.snap_scheduler import (scheduler_enable,
                                               scheduler_init,
                                               scheduler_status,
                                               scheduler_disable,
                                               scheduler_add_jobs,
                                               scheduler_delete,
                                               scheduler_list)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed', 'distributed'],
          ['glusterfs']])
class SnapshotDeleteExistingScheduler(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()
        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts "
                                 "to clients %s" % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):

        # SettingUp volume and Mounting the volume
        self.get_super_method(self, 'setUp')()
        g.log.info("Starting to SetUp and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

        # Enable Shared storage
        g.log.info("Starting to enable shared storage")
        ret = enable_shared_storage(self.mnode)
        if not ret:
            raise ExecutionError("Unexpected: Failed to enable shared storage")
        g.log.info("Successfully enabled shared storage as expected")

        # Validate shared storage mounted
        g.log.info("Starting to validate shared storage mounted")
        count = 0
        while count < 5:
            ret = is_shared_volume_mounted(self.mnode)
            if ret:
                break
            sleep(2)
            count += 1
        if not ret:
            raise ExecutionError("Failed to mount shared volume")
        g.log.info("Successfully mounted shared volume")

        # Validate shared storage is enabled
        g.log.info("Starting to validate shared storage volume")
        volinfo = get_volume_info(self.mnode, "gluster_shared_storage")
        if ((volinfo['gluster_shared_storage']['options']
             ['cluster.enable-shared-storage']) != 'enable'):
            raise ExecutionError("Unexpected: shared storage is disabled")
        g.log.info("Shared storage enabled successfully as expected")

    def tearDown(self):

        # Disable snap scheduler
        g.log.info("Starting to disable snapshot scheduler on all nodes")
        ret, _, _ = scheduler_disable(self.mnode)
        if ret != 0:
            raise ExecutionError("Failed to disable snap scheduler "
                                 "on all nodes")
        g.log.info("Successfully disabled snapshot scheduler on all nodes")

        # Check if shared storage is enabled
        # Disable if true
        g.log.info("Checking if shared storage is mounted")
        ret = is_shared_volume_mounted(self.mnode)
        if ret:
            g.log.info("Disabling shared storage")
            ret = disable_shared_storage(self.mnode)
            if not ret:
                raise ExecutionError("Failed to disable shared storage")
            g.log.info("Successfully disabled shared storage")

        # Unmount and cleanup-volume
        g.log.info("Starting to Unmount and cleanup-volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

    def test_snap_delete_existing_scheduler(self):
        # pylint: disable=too-many-statements
        """
        Description:

        Validating snapshot scheduler behavior when existing schedule
        is deleted.

        Steps:
        * Enable shared volume
        * Create a volume
        * Initialise snap_scheduler on all nodes
        * Enable snap_scheduler
        * Validate snap_scheduler status
        * Perform IO on mounts
        * Schedule a job of creating snapshot every 30 mins
        * Perform snap_scheduler list
        * Delete scheduled job
        * Validate IO is successful
        * Perform snap_scheduler list
        """

        # Initialise snap scheduler
        g.log.info("Initialising snap_scheduler on all servers")
        count = 0
        while count < 80:
            ret = scheduler_init(self.servers)
            if ret:
                break
            sleep(2)
            count += 1
        self.assertTrue(ret, "Failed to initialise scheduler on all servers")
        g.log.info("Successfully initialised scheduler on all servers")

        # Enable snap scheduler
        g.log.info("Enabling snap_scheduler")
        ret, _, _ = scheduler_enable(self.mnode)
        self.assertEqual(ret, 0, "Failed to enable scheduler on node %s"
                         % self.mnode)
        g.log.info("Successfully enabled scheduler on node %s", self.mnode)

        # Validate snapshot scheduler status
        g.log.info("Validating status of snap_scheduler")
        for server in self.servers:
            count = 0
            while count < 40:
                ret, status, _ = scheduler_status(server)
                if status.strip().split(":")[2] == ' Enabled':
                    break
                sleep(2)
                count += 2
        self.assertEqual(status.strip().split(":")[2], ' Enabled',
                         "Failed to validate status of scheduler")
        g.log.info("Successfully validated scheduler status")

        # Write files on all mounts
        g.log.info("Starting IO on all mounts...")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python %s create_files "
                   "-f 10 --base-file-name file %s" % (
                       self.script_upload_path,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Add a job to schedule snapshot every 30 mins
        g.log.info("Starting to add new job")
        self.scheduler = r"*/30 * * * *"
        self.job_name = "Job1"
        ret, _, _ = scheduler_add_jobs(self.mnode, self.job_name,
                                       self.scheduler, self.volname)
        self.assertEqual(ret, 0, "Failed to add job")
        g.log.info("Successfully added Job on volume %s", self.volname)

        # Perform snap_scheduler list
        g.log.info("Starting to list all scheduler jobs")
        ret, _, _ = scheduler_list(self.mnode)
        self.assertEqual(ret, 0, "Failed to list scheduler jobs")
        g.log.info("Successfully listed all jobs")

        # Delete scheduled job
        g.log.info("Starting to delete scheduled jobs")
        ret, _, _ = scheduler_delete(self.mnode, self.job_name)
        self.assertEqual(ret, 0, "Failed to delete scheduled job")
        g.log.info("Successfully deleted scheduled job %s", self.job_name)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # Perform snap_scheduler list (no active jobs should be present)
        g.log.info("Starting to list all scheduler jobs")
        ret, out, _ = scheduler_list(self.mnode)
        self.assertEqual(ret, 0, "Failed to list scheduler jobs")
        ret1 = out.strip().split(":")
        self.assertEqual(ret1[1], " No snapshots scheduled", "Unexpected: "
                         "Jobs are getting listed even after being deleted")
        g.log.info("Expected: No snapshots Jobs scheduled")
