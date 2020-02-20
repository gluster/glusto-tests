#  Copyright (C) 2018-2020  Red Hat, Inc. <http://www.redhat.com>
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
        Test cases in this module tests creation and mount of new volume while
        IO is in progress on another volume
"""
from copy import deepcopy
from glusto.core import Glusto as g

from glustolibs.gluster.nfs_ganesha_libs import (
    NfsGaneshaClusterSetupClass, wait_for_nfs_ganesha_volume_to_get_exported)
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs, get_mounts_stat
from glustolibs.gluster.volume_libs import (
    cleanup_volume, wait_for_volume_process_to_be_online)
from glustolibs.gluster.lib_utils import get_servers_bricks_dict
from glustolibs.gluster.volume_ops import volume_create, volume_start
from glustolibs.gluster.nfs_ganesha_ops import export_nfs_ganesha_volume


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNewVolumeWhileIoInProgress(NfsGaneshaClusterSetupClass):
    """
    Test cases to verify creation, export and mount of new volume while IO is
    going on another volume exported through nfs-ganesha.
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
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        """
        Setup Volume and Mount Volume
        """
        g.log.info("Starting to setup and mount volume %s", self.volname)
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup and mount volume %s"
                                 % self.volname)
        g.log.info("Successful in setup and mount volume %s", self.volname)

    def test_new_volume_while_io_in_progress(self):
        """
        Create, export and mount new volume while IO running on mount of
        another volume
        Steps:
        1. Start IO on mount points
        2. Create another volume 'volume_new'
        3. Export volume_new through nfs-ganesha
        4. Mount the volume on clients
        """
        # pylint: disable=too-many-statements, too-many-locals
        # Start IO on all mount points
        all_mounts_procs = []
        count = 1
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        self.volname_new = '%s_new' % self.volname
        kwargs = {}
        dict_index = 0

        # Creating mounts list for mounting new volume
        self.mounts_new = []
        for mount_obj in self.mounts:
            self.mounts_new.append(deepcopy(mount_obj))
        for mount_obj in self.mounts_new:
            mount_obj.volname = self.volname_new
            mount_obj.mountpoint = '%s_new' % mount_obj.mountpoint

        # Fetch details for creating a replicate volume.
        replica_count = (
            self.default_volume_type_config['replicated']['replica_count'])
        servers_bricks_dict = get_servers_bricks_dict(self.all_servers,
                                                      self.all_servers_info)
        bricks_list = []
        kwargs['replica_count'] = replica_count
        kwargs['transport_type'] = (
            self.default_volume_type_config['replicated']['transport'])

        for num in range(0, replica_count):
            # Current_server is the server on which brick path will be created
            current_server = list(servers_bricks_dict.keys())[dict_index]
            current_server_unused_bricks_list = (
                list(servers_bricks_dict.values())[dict_index])
            if current_server_unused_bricks_list:
                brick_path = ("%s:%s/%s_brick%s" %
                              (current_server,
                               current_server_unused_bricks_list[0],
                               self.volname_new, num))
                bricks_list.append(brick_path)

                # Remove the added brick from the list
                list(servers_bricks_dict.values())[dict_index].pop(0)

            if dict_index < len(servers_bricks_dict) - 1:
                dict_index = dict_index + 1
            else:
                dict_index = 0

        # Create volume 'volume_new'
        ret, _, _ = volume_create(mnode=self.mnode, volname=self.volname_new,
                                  bricks_list=bricks_list, force=False,
                                  **kwargs)
        self.assertEqual(ret, 0, "Unable to create volume %s"
                         % self.volname_new)
        g.log.info("Successfully created volume %s", self.volname_new)

        ret, _, _ = volume_start(self.mnode, self.volname_new)
        self.assertEqual(ret, 0, "Unable to start volume %s"
                         % self.volname_new)

        # Wait for volume processes to be online
        g.log.info("Wait for volume %s processes to be online",
                   self.volname_new)
        ret = wait_for_volume_process_to_be_online(self.mnode,
                                                   self.volname_new)
        self.assertTrue(ret, "Wait timeout: Processes of volume %s are "
                             "not online." % self.volname_new)
        g.log.info("Volume processes of volume %s are now online",
                   self.volname_new)

        # Export volume as nfs-ganesha export
        ret, _, _ = export_nfs_ganesha_volume(self.mnode, self.volname_new)
        self.assertEqual(ret, 0, "Failed to set ganesha.enable 'on' on "
                                 "volume %s" % self.volname_new)
        g.log.info("Successful in setting ganesha.enable to 'on' on "
                   "volume %s", self.volname_new)

        # Verify volume export
        ret = wait_for_nfs_ganesha_volume_to_get_exported(self.mnode,
                                                          self.volname_new)
        self.assertTrue(ret, "Failed to export volume %s as nfs-ganesha "
                             "export" % self.volname_new)
        g.log.info("Successfully exported volume %s", self.volname_new)

        # Mount the new volume
        for mount_obj in self.mounts_new:
            ret = mount_obj.mount()
            self.assertTrue(ret, ("Failed to mount %s on client"
                                  " %s" % (mount_obj.volname,
                                           mount_obj.client_system)))
            g.log.info("Successfully mounted %s on client %s",
                       mount_obj.volname,
                       mount_obj.client_system)

        # Verify mounts
        for mount_obj in self.mounts_new:
            ret = mount_obj.is_mounted()
            self.assertTrue(ret, ("Volume %s is not mounted on client"
                                  " %s" % (mount_obj.volname,
                                           mount_obj.client_system)))
            g.log.info("Verified: Volume %s is mounted on client %s",
                       mount_obj.volname,
                       mount_obj.client_system)
        g.log.info("Export and mount of new volume %s is success.",
                   self.volname_new)

        # Validate IO
        g.log.info("Validating IO's")
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all IO")

        # Get stat of all the files/dirs created.
        g.log.info("Get stat of all the files/dirs created.")
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfully got stat of all files/dirs created")

    def tearDown(self):
        """
        Unmount and cleanup the volumes
        """
        # Unmount volumes
        all_mounts = self.mounts + self.mounts_new
        for mount_obj in all_mounts:
            ret = mount_obj.unmount()
            if ret:
                g.log.info("Successfully unmounted volume %s from %s",
                           mount_obj.volname, mount_obj.client_system)
            else:
                g.log.error("Failed to unmount volume %s from %s",
                            mount_obj.volname, mount_obj.client_system)

        # Cleanup volumes
        for volume in self.volname, self.volname_new:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Failed to cleanup volume %s", volume)
            g.log.info("Volume %s deleted successfully", volume)

    @classmethod
    def tearDownClass(cls):
        cls.get_super_method(cls, 'tearDownClass')(
            delete_nfs_ganesha_cluster=False)
