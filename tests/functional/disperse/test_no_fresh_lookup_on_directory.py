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

from random import choice

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.brickdir import file_exists
from glustolibs.gluster.mount_ops import mount_volume, umount_volume
from glustolibs.gluster.volume_libs import set_volume_options, get_subvols
from glustolibs.gluster.glusterfile import occurences_of_pattern_in_file
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           get_online_bricks_list)
from glustolibs.gluster.volume_ops import volume_start
from glustolibs.gluster.volume_libs import wait_for_volume_process_to_be_online


@runs_on([['distributed-dispersed', 'distributed-replicated',
           'distributed-arbiter'], ['glusterfs']])
class TestNoFreshLookUpBrickDown(GlusterBaseClass):

    def setUp(self):
        """
        setUp method for every test
        """
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setting client log-level to Debug
        self.volume['options'] = {'diagnostics.client-log-level': 'DEBUG'}

        # Creating Volume and mounting
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Volume creation failed: %s" % self.volname)
        g.log.info("Volume is created and started")

    def tearDown(self):
        """
        tearDown method for every test
        """
        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

        ret = umount_volume(mclient=self.mounts[0].client_system,
                            mpoint=self.mountpoint)
        if not ret:
            raise ExecutionError("Unable to umount the volume")
        g.log.info("Unmounting of the volume %s succeeded", self.volname)

        # Resetting the volume option set in the setup
        ret = set_volume_options(self.mnode, self.volname,
                                 {'diagnostics.client-log-level': 'INFO'})
        if not ret:
            raise ExecutionError("Unable to set the client log level to INFO")
        g.log.info("Volume option is set successfully.")

        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Unable to perform volume clenaup")
        g.log.info("Volume cleanup is successfull")

    def do_lookup(self, dirname):
        """
        Performes a look up on the directory.
        """
        ret = file_exists(self.mounts[0].client_system, dirname)
        self.assertTrue(ret, "Directory %s doesn't exists " % dirname)
        g.log.info("Directory present on the %s",
                   self.mounts[0].client_system)

    def match_occurences(self, first_count, search_pattern, filename):
        """
        Validating the count of the search pattern before and after
        lookup.
        """
        newcount = occurences_of_pattern_in_file(self.mounts[0].client_system,
                                                 search_pattern, filename)
        self.assertEqual(first_count, newcount, "Failed: The lookups logged"
                         " for the directory <dirname> are more than expected")
        g.log.info("No more new lookups for the dir1")

    def test_no_fresh_lookup(self):
        """
        The testcase covers negative lookup of a directory in distributed-
        replicated and distributed-dispersed volumes
        1. Mount the volume on one client.
        2. Create a directory
        3. Validate the number of lookups for the directory creation from the
           log file.
        4. Perform a new lookup of the directory
        5. No new lookups should have happened on the directory, validate from
           the log file.
        6. Bring down one subvol of the volume and repeat step 4, 5
        7. Bring down one brick from the online bricks and repeat step 4, 5
        8. Start the volume with force and wait for all process to be online.
        """

        # Mounting the volume on a distinct directory for the validation of
        # testcase
        self.mountpoint = "/mnt/" + self.volname
        ret, _, _ = mount_volume(self.volname, mtype=self.mount_type,
                                 mpoint=self.mountpoint,
                                 mserver=self.mnode,
                                 mclient=self.mounts[0].client_system)
        self.assertEqual(ret, 0, ("Volume %s is not mounted") % self.volname)
        g.log.info("Volume mounted successfully : %s", self.volname)

        # Distinct log file for the validation of this test
        filename = "/var/log/glusterfs/mnt-" + self.volname + ".log"
        # Creating a dir on the mount point.
        dirname = self.mountpoint + "/dir1"
        ret = mkdir(host=self.mounts[0].client_system, fqpath=dirname)
        self.assertTrue(ret, "Failed to create dir1")
        g.log.info("dir1 created successfully for %s",
                   self.mounts[0].client_system)

        search_pattern = "/dir1: Calling fresh lookup"

        # Check log file for the pattern in the log file
        first_count = occurences_of_pattern_in_file(
            self.mounts[0].client_system, search_pattern, filename)
        self.assertGreater(first_count, 0, "Unable to find "
                           "pattern in the given file")
        g.log.info("Searched for the pattern in the log file successfully")

        # Perform a lookup of the directory dir1
        self.do_lookup(dirname)

        # Recheck for the number of lookups from the log file
        self.match_occurences(first_count, search_pattern, filename)

        # Bring down one subvol of the volume
        ret = get_subvols(self.mnode, self.volname)
        brick_list = choice(ret['volume_subvols'])
        ret = bring_bricks_offline(self.volname, brick_list)
        self.assertTrue(ret, "Unable to bring the given bricks offline")
        g.log.info("Able to bring all the bricks in the subvol offline")

        # Do a lookup on the mountpoint for the directory dir1
        self.do_lookup(dirname)

        # Re-check the number of occurences of lookup
        self.match_occurences(first_count, search_pattern, filename)

        # From the online bricks, bring down one brick
        online_bricks = get_online_bricks_list(self.mnode, self.volname)
        self.assertIsNotNone(online_bricks, "Unable to fetch online bricks")
        g.log.info("Able to fetch the online bricks")
        offline_brick = choice(online_bricks)
        ret = bring_bricks_offline(self.volname, [offline_brick])
        self.assertTrue(ret, "Unable to bring the brick %s offline " %
                        offline_brick)
        g.log.info("Successfully brought the brick %s offline", offline_brick)

        # Do a lookup on the mounpoint and check for new lookups from the log
        self.do_lookup(dirname)
        self.match_occurences(first_count, search_pattern, filename)

        # Start volume with force
        ret, _, err = volume_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, "Unable to force start the volume %s " % err)
        g.log.info("Volume started successfully")

        # Wait for all the processess to be online.
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, "Some processes are offline")
        g.log.info("All processes of the volume")
