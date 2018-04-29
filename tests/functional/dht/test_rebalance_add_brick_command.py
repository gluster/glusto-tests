#  Copyright (C) 2017-2018 Red Hat, Inc. <http://www.redhat.com>
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

"""Positive test - Exercise Add-brick command"""

from glusto.core import Glusto as g

from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import (cleanup_volume, expand_volume,
                                            volume_stop)
from glustolibs.gluster.volume_ops import get_volume_list
from glustolibs.io.utils import validate_io_procs
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['dispersed', 'replicated', 'distributed', 'distributed-dispersed',
           'distributed-replicated'],
          ['glusterfs']])
class ExerciseAddbrickCommand(GlusterBaseClass):
    """Positive test - Exercise Add-brick command"""

    def setUp(self):
        """Setup Volume"""
        # Calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        # Setup Volume Volume
        g.log.info("Starting to Setup Volume")
        ret = self.setup_volume()
        if not ret:
            g.log.error("Failed to Setup")
            raise ExecutionError("Failed to Setup Volume")

    def test_add_brick_running_volume(self):
        """Add brick to running volume
        """
        vol_bricks = {
            'before': get_all_bricks(self.mnode, self.volname),
            'after': []}

        g.log.debug("Expanding volume %s", self.volname)
        ret = expand_volume(mnode=self.mnode,
                            volname=self.volname,
                            servers=self.servers,
                            all_servers_info=self.all_servers_info)

        self.assertTrue(ret, 'Unable to expand volume %s' % self.volname)
        g.log.info("Add brick successfully %s", self.volname)

        vol_bricks['after'] = get_all_bricks(self.mnode, self.volname)

        self.assertGreater(len(vol_bricks['after']), len(vol_bricks['before']),
                           "Expected new volume size to be greater than old")

        g.log.debug("Expanding volume %s", self.volname)
        ret = expand_volume(mnode=self.mnode,
                            volname=self.volname,
                            servers=self.servers,
                            all_servers_info=self.all_servers_info)
        self.assertTrue(ret, 'Unable to expand volume %s' % self.volname)

        g.log.info("Success on expanding volume %s", self.volname)

        vol_bricks['before'] = vol_bricks['after']
        vol_bricks['after'] = get_all_bricks(self.mnode, self.volname)
        self.assertGreater(len(vol_bricks['after']), len(vol_bricks['before']),
                           "Expected new volume size to be greater than old")

        g.log.info('Success in add bricks to running volume')

    def test_add_bricks_stopped_volume(self):
        """Add bricks to stopped volume
        """
        ret, _, _ = volume_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Can't stop volume")

        g.log.info('Volume %s stopped successfully', self.volname)

        vol_bricks = {
            'before': get_all_bricks(self.mnode, self.volname),
            'after': []
        }

        g.log.debug("Adding bricks to volume %s", self.volname)
        ret = expand_volume(mnode=self.mnode,
                            volname=self.volname,
                            servers=self.servers,
                            all_servers_info=self.all_servers_info)
        self.assertTrue(ret, 'Unable to expand volume %s' % self.volname)

        g.log.info('Added bricks to stopped volume %s', self.volname)

        vol_bricks['after'] = get_all_bricks(self.mnode, self.volname)
        self.assertGreater(len(vol_bricks['after']), len(vol_bricks['before']),
                           "Expected new volume size to be greater than old")
        g.log.info('Success in add bricks to stopped volume')

    def test_add_bricks_io_mount_point(self):
        # Mount volume
        ret = self.mount_volume(self.mounts)
        self.assertTrue(ret, 'Mount volume: FAIL')
        g.log.info('Mounted Volume %s: Success', self.volname)

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", self.clients)
        script_location = "/usr/share/glustolibs/io/scripts/file_dir_ops.py"
        ret = upload_scripts(self.clients, script_location)
        if not ret:
            clients = ", ".join(self.clients)
            g.log.error("Failed to upload IO scripts to clients %s",
                        clients)
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   self.clients)
        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        for index, mount_obj in enumerate(self.mounts, start=1):
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 2 "
                   "--max-num-of-dirs 2 "
                   "--num-of-files 10 %s" % (script_location,
                                             index + 10,
                                             mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            # Expand volume
            g.log.debug("Expanding volume %s", self.volname)
            ret = expand_volume(mnode=self.mnode,
                                volname=self.volname,
                                servers=self.servers,
                                all_servers_info=self.all_servers_info)
            self.assertTrue(ret, "Expand volume %s: Fail" % self.volname)
            g.log.info("Volume %s expanded: Success", self.volname)

            # Validate IO on current mount point
            g.log.debug('Validating IO on mount point %s:%s',
                        mount_obj.client_system, mount_obj.mountpoint)
            self.assertTrue(validate_io_procs([proc], [mount_obj]),
                            'IO Failed on client %s:%s' %
                            (mount_obj.client_system, mount_obj.mountpoint))
            g.log.info("IO is successful on mount point %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)

        g.log.debug("Unmounting mount points")
        self.assertTrue(self.unmount_volume(self.mounts),
                        'Unmount end points: Fail')
        g.log.info("Unmount mount points: Success")

        g.log.info('Add brick during IO operations successfully')

    def tearDown(self):
        """tear Down callback"""
        # clean up all volumes
        vol_list = get_volume_list(self.mnode)
        if vol_list is None:
            raise ExecutionError("Failed to get the volume list")
        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Unable to delete volume % s" % volume)
            g.log.info("Volume deleted successfully : %s", volume)

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)
