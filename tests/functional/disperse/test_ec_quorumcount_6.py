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

"""
Test Description:
    Test Disperse Quorum Count Set to 6
"""
from random import sample, choice
from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.brick_libs import (bring_bricks_online,
                                           wait_for_bricks_to_be_online,
                                           get_offline_bricks_list,
                                           bring_bricks_offline)
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_ops import (volume_reset,
                                           set_volume_options)
from glustolibs.gluster.volume_libs import (
    log_volume_info_and_status, expand_volume,
    get_subvols)


@runs_on([['dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestEcQuorumCount6(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        Cleanup and umount volume
        """
        # Cleanup and umount volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_ec_quorumcount_6(self):
        """
        Test Steps:
        - Write IO's when all bricks are online
        - Get subvol from which bricks to be brought down
        - Set volume disperse quorum count to 6
        - Start writing and reading IO's
        - Bring a brick down,say b1
        - Validate write has failed and read is successful
        - Start IO's again while quorum is not met on volume
          write should fail and read should pass
        - Add-brick and log
        - Start Rebalance
        - Wait for rebalance,which should fail as quorum is not met
        - Bring brick online
        - Wait for brick to come online
        - Check if bricks are online
        - Start IO's again when all bricks are online
        - IO's should complete successfully
        - Start IO's again and reset volume
        - Bring down other bricks to max redundancy
        - Validating IO's and waiting to complete
        """

        # pylint: disable=too-many-branches,too-many-statements,too-many-locals

        mountpoint = self.mounts[0].mountpoint
        client1 = self.mounts[0].client_system
        client2 = self.mounts[1].client_system

        # Write IO's  when all bricks are online
        writecmd = ("cd %s; for i in `seq 1 100` ;"
                    "do dd if=/dev/urandom of=file$i bs=1M "
                    "count=5;done" % mountpoint)

        # IO's should complete successfully
        ret, _, err = g.run(client1, writecmd)
        self.assertEqual(ret, 0, err)
        g.log.info('Finished writes on files sucessfully')

        # Select a subvol from which bricks to be brought down
        sub_vols = get_subvols(self.mnode, self.volname)
        bricks_list1 = list(choice(sub_vols['volume_subvols']))
        brick_1 = sample(bricks_list1, 1)

        # Set volume disperse quorum count to 6
        ret = set_volume_options(self.mnode, self.volname,
                                 {"disperse.quorum-count": "6"})
        self.assertTrue(ret, 'Failed to set volume {}'
                        ' options'.format(self.volname))
        g.log.info('Successfully set disperse quorum on %s', self.volname)

        # Start writing and reading IO's
        procwrite, procread, count = [], [], 1
        for mount_obj in self.mounts:
            writecmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                        "--dirname-start-num %d --dir-depth 1 "
                        "--dir-length 10 --max-num-of-dirs 1 "
                        "--num-of-files 10 %s" % (
                            self.script_upload_path, count,
                            mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, writecmd,
                               user=mount_obj.user)
            procwrite.append(proc)
            count = count + 10

        readcmd = ("cd %s; for i in `seq 1 100` ;"
                   "do dd if=file$i of=/dev/null bs=1M "
                   "count=5;done" % mountpoint)
        ret = g.run_async(client2, readcmd)
        procread.append(ret)

        # Brick 1st brick down
        ret = bring_bricks_offline(self.volname,
                                   brick_1)
        self.assertTrue(ret, 'Brick {} is not offline'.format(brick_1))
        g.log.info('Brick %s is offline successfully', brick_1)

        # Validate write has failed and read is successful
        ret = validate_io_procs(procwrite, self.mounts)
        self.assertFalse(ret, 'Write successful even after disperse quorum is '
                         'not met')
        g.log.info('EXPECTED - Writes failed as disperse quroum is not met')

        ret = validate_io_procs(procread, self.mounts[1])
        self.assertTrue(ret, 'Read operation failed on the client')
        g.log.info('Reads on files successful')

        # Start IO's again while quorum is not met on volume

        writecmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                    "--dirname-start-num 20 --dir-depth 1 "
                    "--dir-length 10 --max-num-of-dirs 1 "
                    "--num-of-files 10 %s" % (
                        self.script_upload_path,
                        mountpoint))
        readcmd = ("cd %s; for i in `seq 1 100` ;"
                   "do dd if=file$i of=/dev/null bs=1M "
                   "count=5;done" % mountpoint)

        ret, _, err = g.run(client1, writecmd)
        self.assertNotEqual(ret, 0, 'Writes passed even after disperse quorum '
                            'not met')
        g.log.info('Expected: Writes failed as disperse quorum is not '
                   'met with %s error', err)

        ret, _, err = g.run(client2, readcmd)
        self.assertEqual(ret, 0, err)
        g.log.info('Reads on files successful')

        # Add brick
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info, force=True)
        self.assertTrue(ret, ("Failed to expand the volume {}".format
                              (self.volname)))
        g.log.info("Expanding volume %s is successful", self.volname)

        # Log Volume Info and Status after expanding the volume
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume {}".format(self.volname)))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Start Rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ('Rebalance failed on the volume'
                                  ' {}'.format(self.volname)))
        g.log.info('Rebalance has started on volume %s',
                   self.volname)

        # Wait for rebalance to complete
        # Which should also fail as quorum is not met
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=600)
        self.assertFalse(ret, "Rebalance passed though disperse quorum "
                              "is not met on volume")
        g.log.info("Expected: Rebalance failed on the volume %s,disperse"
                   " quorum is not met", self.volname)

        # Bring brick online
        ret = bring_bricks_online(self.mnode, self.volname,
                                  brick_1)
        self.assertTrue(ret, 'Brick not brought online')
        g.log.info('Brick brought online successfully')

        # Wait for brick to come online
        ret = wait_for_bricks_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, 'Bricks are not online')
        g.log.info('EXPECTED : Bricks are online')

        # Check if bricks are online
        ret = get_offline_bricks_list(self.mnode, self.volname)
        self.assertListEqual(ret, [], 'All bricks are not online')
        g.log.info('All bricks are online')

        # Start IO's again when all bricks are online
        writecmd = ("cd %s; for i in `seq 101 200` ;"
                    "do dd if=/dev/urandom of=file$i bs=1M "
                    "count=5;done" % mountpoint)
        readcmd = ("cd %s; for i in `seq 101 200` ;"
                   "do dd if=file$i of=/dev/null bs=1M "
                   "count=5;done" % mountpoint)

        # IO's should complete successfully
        ret, _, err = g.run(client1, writecmd)
        self.assertEqual(ret, 0, err)
        g.log.info('Writes on client % successful', client1)

        ret, _, err = g.run(client2, readcmd)
        self.assertEqual(ret, 0, err)
        g.log.info('Read on client % successful', client2)

        # Start IO's again
        all_mounts_procs, count = [], 30
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d --dir-depth 2 "
                   "--dir-length 10 --max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        # Reset volume
        ret, _, err = volume_reset(self.mnode, self.volname)
        self.assertEqual(ret, 0, err)
        g.log.info('Reset of volume %s successful', self.volname)

        # Bring down other bricks to max redundancy
        # Bringing bricks offline
        bricks_to_offline = sample(bricks_list1, 2)
        ret = bring_bricks_offline(self.volname,
                                   bricks_to_offline)
        self.assertTrue(ret, 'Redundant bricks not offline')
        g.log.info('Redundant bricks are offline successfully')

        # Validating IO's and waiting to complete
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, 'IO failed on some of the clients')
        g.log.info("Successfully validated all IO's")
