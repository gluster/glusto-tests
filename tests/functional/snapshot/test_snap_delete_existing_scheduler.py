#  Copyright (C) 2017-2018  Red Hat, Inc. <http://www.redhat.com>
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
Description:

    Test Cases in this module tests the
    snapshot scheduler behavior when shared volume is mounted/not
    mounted. scheduler command such as initialise scheduler,
    enable scheduler, status of scheduler.
"""
import time
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.volume_ops import get_volume_info
from glustolibs.gluster.shared_storage_ops import (enable_shared_storage,
                                                   is_shared_volume_mounted,
                                                   is_shared_volume_unmounted,
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
        GlusterBaseClass.setUpClass.im_func(cls)
        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, script_local_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts "
                                 "to clients %s" % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):

        # SettingUp volume and Mounting the volume
        GlusterBaseClass.setUp.im_func(self)
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
        ret = is_shared_volume_mounted(self.mnode)
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

        # disable shared storage
        g.log.info("starting to disable shared storage")
        count = 0
        while count < 80:
            ret = disable_shared_storage(self.mnode)
            if ret:
                break
            time.sleep(2)
            count += 1

        if not ret:
            raise ExecutionError("Unexpected: Failed to disable "
                                 "shared storage")
        g.log.info("Expected: Successfully disabled shared storage")

        # Validate shared volume unmounted
        g.log.info("Validate shared volume unmounted")
        ret = is_shared_volume_unmounted(self.mnode)
        if not ret:
            raise ExecutionError("Failed to unmount shared storage")
        g.log.info("Successfully unmounted shared storage")

        # Unmount and cleanup-volume
        g.log.info("Starting to Unmount and cleanup-volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

    def test_snap_delete_existing_scheduler(self):
        # pylint: disable=too-many-statements
        """
        Steps:
        1. enable shared volume
        2. create a volume
        3. initialise snap scheduler on all nodes
        4. enable snap scheduler
        5. check snap scheduler status
        6. perform io on mounts
        7. schedule a job of creating snapshot
           every 30 mins
        8. list jobs created
        9. delete scheduled job
        10. validate io is successful
        11. list job should not list
            any existing snapshot jobs
        """

        # Initialise snap scheduler
        g.log.info("Initialising snap scheduler on all servers")
        count = 0
        while count < 80:
            ret = scheduler_init(self.servers)
            if ret:
                break
            time.sleep(2)
            count += 1
        self.assertTrue(ret, "Failed to initialise scheduler on all servers")
        g.log.info("Successfully initialised scheduler on all servers")

        # Enable snap scheduler
        g.log.info("Enabling snap scheduler")
        ret, _, _ = scheduler_enable(self.mnode)
        self.assertEqual(ret, 0, "Failed to enable scheduler on node %s"
                         % self.mnode)
        g.log.info("Successfully enabled scheduler on node %s", self.mnode)

        # Check snapshot scheduler status
        g.log.info("checking status of snapshot scheduler")
        for server in self.servers:
            count = 0
            while count < 40:
                ret, status, _ = scheduler_status(server)
                if status.strip().split(":")[2] == ' Enabled':
                    break
                time.sleep(2)
                count += 2
        self.assertEqual(status.strip().split(":")[2], ' Enabled',
                         "Failed to check status of scheduler")
        g.log.info("Successfuly checked scheduler status")

        # write files on all mounts
        g.log.info("Starting IO on all mounts...")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name file %s" % (self.script_upload_path,
                                                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # add a job to schedule snapshot every 30 mins
        g.log.info("Starting to add new job")
        self.scheduler = r"*/30 * * * *"
        self.job_name = "Job1"
        ret, _, _ = scheduler_add_jobs(self.mnode, self.job_name,
                                       self.scheduler,
                                       self.volname)
        self.assertEqual(ret, 0, "Failed to add job")
        g.log.info("Successfully added Job on volume %s", self.volname)

        # scheduler list
        g.log.info("Starting to list all scheduler jobs")
        ret, _, _ = scheduler_list(self.mnode)
        self.assertEqual(ret, 0, "Failed to list scheduler jobs")
        g.log.info("Successfully listed all jobs")

        # delete scheduled job
        g.log.info("Starting to delete scheduled jobs")
        ret, _, _ = scheduler_delete(self.mnode, self.job_name)
        self.assertEqual(ret, 0, "Failed to delete scheduled job")
        g.log.info("Successfully deleted scheduled job %s",
                   self.job_name)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # scheduler list (no active jobs should be there)
        g.log.info("Starting to list all scheduler jobs")
        ret, out, _ = scheduler_list(self.mnode)
        self.assertEqual(ret, 0, "Failed to list scheduler jobs")
        ret1 = out.strip().split(":")
        self.assertEqual(ret1[1], " No snapshots scheduled", "Unexpected:"
                         "Failed to delete scheduled job %s" % self.job_name)
        g.log.info("Expected: No snapshots Jobs scheduled")
