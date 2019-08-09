#  Copyright (C) 2019  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_init import (start_glusterd, stop_glusterd,
                                             restart_glusterd)
from glustolibs.gluster.gluster_init import is_glusterd_running
from glustolibs.gluster.glusterfile import (move_file,
                                            find_and_replace_in_file,
                                            check_if_pattern_in_file)
from glustolibs.misc.misc_libs import daemon_reload


class TestVolumeOptionSetWithMaxcharacters(GlusterBaseClass):

    def setUp(self):

        GlusterBaseClass.setUp.im_func(self)

        # check whether peers are in connected state
        ret = self.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Peers are not in connected state")
        g.log.info("Peers are in connected state.")

    def tearDown(self):

        # Stop glusterd
        ret = stop_glusterd(self.mnode)
        if not ret:
            raise ExecutionError("Failed to stop glusterd on %s"
                                 % self.mnode)
        g.log.info("Successfully stopped glusterd.")

        # Reverting log level in /usr/lib/systemd/system/glusterd.service
        # to INFO
        glusterd_file = "/usr/lib/systemd/system/glusterd.service"
        ret = find_and_replace_in_file(self.mnode, 'LOG_LEVEL=DEBUG',
                                       'LOG_LEVEL=INFO', glusterd_file)
        if not ret:
            raise ExecutionError("Changes")
        g.log.info("Changes to glusterd.services reverted.")

        # Archiving the glusterd log file of test case.
        ret = move_file(self.mnode,
                        '/var/log/glusterfs/glusterd.log',
                        '/var/log/glusterfs/EnableDebugMode-glusterd.log')
        if not ret:
            raise ExecutionError("Archiving present log file failed.")
        g.log.info("Archiving present log file successful.")

        # Reverting back to old glusterd log file.
        ret = move_file(self.mnode,
                        '/var/log/glusterfs/old.log',
                        '/var/log/glusterfs/glusterd.log')
        if not ret:
            raise ExecutionError("Reverting glusterd log failed.")
        g.log.info("Reverting of glusterd log successful.")

        # Daemon should be reloaded as unit file is changed
        ret = daemon_reload(self.mnode)
        if not ret:
            raise ExecutionError("Unable to reload the daemon")
        g.log.info("Daemon reloaded successfully")

        # Restart glusterd
        ret = start_glusterd(self.mnode)
        if not ret:
            raise ExecutionError("Failed to start glusterd on %s"
                                 % self.mnode)
        g.log.info("Successfully restarted glusterd.")

        # Checking if glusterd is running on all nodes or not.
        count = 0
        while count < 60:
            ret = is_glusterd_running(self.mnode)
            if not ret:
                break
            sleep(2)
            count += 1
        if ret:
            raise ExecutionError("glusterd is not running on %s"
                                 % self.mnode)
        g.log.info("glusterd running with log level INFO.")

        # Checking if peers are in connected state or not.
        count = 0
        while count < 60:
            ret = self.validate_peers_are_connected()
            if ret:
                break
            sleep(3)
            count += 1
        if not ret:
            raise ExecutionError("Peers are not in connected state.")
        g.log.info("Peers are in connected state.")

        GlusterBaseClass.tearDown.im_func(self)

    def test_enabling_gluster_debug_mode(self):

        # pylint: disable=too-many-statements
        """
        Testcase:
        1. Stop glusterd.
        2. Change log level to DEBUG in
           /usr/local/lib/systemd/system/glusterd.service.
        3. Remove glusterd log
        4. Start glusterd
        5. Issue some gluster commands
        6. Check for debug messages in glusterd log
        """
        # Stop glusterd
        ret = stop_glusterd(self.mnode)
        self.assertTrue(ret, "Failed to stop glusterd on %s"
                        % self.mnode)
        g.log.info("Successfully stopped glusterd.")

        # Change log level in /usr/lib/systemd/system/glusterd.service
        # to DEBUG
        glusterd_file = "/usr/lib/systemd/system/glusterd.service"
        ret = find_and_replace_in_file(self.mnode, 'LOG_LEVEL=INFO',
                                       'LOG_LEVEL=DEBUG', glusterd_file)
        self.assertTrue(ret, "Unable to change Log_LEVEL to DEBUG.")

        # Archive old glusterd.log file.
        ret = move_file(self.mnode,
                        '/var/log/glusterfs/glusterd.log',
                        '/var/log/glusterfs/old.log')
        self.assertTrue(ret, "Renaming the glusterd log is failed")
        g.log.info("Successfully renamed glusterd.log file.")

        # Daemon reloading as the unit file of the daemon changed
        ret = daemon_reload(self.mnode)
        self.assertTrue(ret, "Daemon reloaded successfully")

        # Start glusterd
        ret = start_glusterd(self.mnode)
        self.assertTrue(ret, "Failed to start glusterd on %s"
                        % self.mnode)
        g.log.info('Successfully to started glusterd.')

        # Check if glusterd is running or not.
        count = 0
        while count < 60:
            ret = is_glusterd_running(self.mnode)
            if ret:
                break
            sleep(2)
            count += 1
        self.assertEqual(ret, 0, "glusterd is not running on %s" % self.mnode)
        g.log.info('glusterd is running after changing log_level to debug.')

        # Instead of executing commands in loop, if glusterd is restarted in
        # one of the nodes in the cluster the handshake messages
        # will be in debug mode.
        ret = restart_glusterd(self.servers[1])
        self.assertTrue(ret, "restarted successfully")

        count = 0
        while count < 60:
            ret = is_glusterd_running(self.mnode)
            if ret:
                break
            sleep(2)
            count += 1
        self.assertEqual(ret, 0, "glusterd is not running on %s" % self.mnode)
        g.log.info('glusterd is running after changing log_level to debug.')

        # Check glusterd logs for debug messages
        glusterd_log_file = "/var/log/glusterfs/glusterd.log"
        ret = check_if_pattern_in_file(self.mnode, ' D ',
                                       glusterd_log_file)
        self.assertEqual(ret, 0, "Debug messages are not present in log.")
        g.log.info("Debug messages are present in log.")
