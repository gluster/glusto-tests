#  Copyright (C) 2015-2018  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online,
    wait_for_volume_process_to_be_online)
from glustolibs.gluster.brick_libs import (select_bricks_to_bring_offline,
                                           bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline)
from glustolibs.gluster.heal_libs import (
    monitor_heal_completion,
    is_heal_complete,
    is_volume_in_split_brain,
    is_shd_daemonized)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (collect_mounts_arequal, validate_io_procs)


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'cifs', 'nfs']])
class TestSelfHeal(GlusterBaseClass):
    """
    Description:
        Arbiter Test cases related to
        healing in default configuration of the volume
    """

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Overriding the volume type to specifically test the volume type
        # Change from distributed-replicated to arbiter
        if cls.volume_type == "distributed-replicated":
            cls.volume['voltype'] = {
                'type': 'distributed-replicated',
                'dist_count': 2,
                'replica_count': 3,
                'arbiter_count': 1,
                'transport': 'tcp'}

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on mounts",
                   cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, [script_local_path])
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        # Calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        Cleanup and umount volume
        """

        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        GlusterBaseClass.tearDown.im_func(self)

    def test_data_self_heal_algorithm_diff_default(self):
        """
        Test Volume Option - 'cluster.data-self-heal-algorithm' : 'diff'

        Description:
        - set the volume option "data-self-heal-algorithm" to value "diff"
        - create IO
        - bring down all bricks processes from selected set
        - modify the data
        - calculate arequal
        - bring bricks online
        - start healing
        - calculate arequal and compare with arequal before bringing bricks
        offline and after bringing bricks online
        """
        # pylint: disable=too-many-locals,too-many-statements
        # Setting options
        g.log.info('Setting options "data-self-heal-algorithm": "diff"...')
        options = {"data-self-heal-algorithm": "diff"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options')
        g.log.info("Option 'data-self-heal-algorithm' is set to 'diff' "
                   "successfully")

        # Creating files on client side
        all_mounts_procs = []
        g.log.info("Generating data for %s:%s",
                   self.mounts[0].client_system, self.mounts[0].mountpoint)
        # Creating files
        command = ("python %s create_files -f 100 %s"
                   % (self.script_upload_path, self.mounts[0].mountpoint))

        proc = g.run_async(self.mounts[0].client_system, command,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # Select bricks to bring offline
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = filter(None, (
            bricks_to_bring_offline_dict['hot_tier_bricks'] +
            bricks_to_bring_offline_dict['cold_tier_bricks'] +
            bricks_to_bring_offline_dict['volume_bricks']))

        # Bring brick offline
        g.log.info('Bringing bricks %s offline...', bricks_to_bring_offline)
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        bricks_to_bring_offline)

        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % bricks_to_bring_offline)
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_to_bring_offline)

        # Modify the data
        all_mounts_procs = []
        g.log.info("Modifying data for %s:%s",
                   self.mounts[0].client_system, self.mounts[0].mountpoint)
        command = ("python %s create_files -f 100 --fixed-file-size 1M %s"
                   % (self.script_upload_path, self.mounts[0].mountpoint))

        proc = g.run_async(self.mounts[0].client_system, command,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # Get arequal before getting bricks online
        g.log.info('Getting arequal before getting bricks online...')
        ret, result_before_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks online '
                   'is successful')

        # Bring brick online
        g.log.info('Bringing bricks %s online...', bricks_to_bring_offline)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_to_bring_offline)
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Verify volume's all process are online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online"
                              % self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Wait for self-heal-daemons to be online
        g.log.info("Waiting for self-heal-daemons to be online")
        ret = is_shd_daemonized(self.all_servers)
        self.assertTrue(ret, "Either No self heal daemon process found")
        g.log.info("All self-heal-daemons are online")

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')

        # Check if heal is completed
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not complete')
        g.log.info('Heal is completed successfully')

        # Check for split-brain
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertFalse(ret, 'Volume is in split-brain state')
        g.log.info('Volume is not in split-brain state')

        # Get arequal after getting bricks online
        g.log.info('Getting arequal after getting bricks online...')
        ret, result_after_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks online '
                   'is successful')

        # Checking arequals before bringing bricks online
        # and after bringing bricks online
        self.assertItemsEqual(result_before_online, result_after_online,
                              'Checksums are not equal')
        g.log.info('Checksums before bringing bricks online '
                   'and after bringing bricks online are equal')
