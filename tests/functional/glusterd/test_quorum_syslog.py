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

from time import sleep
import re

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import (setup_volume, cleanup_volume)
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.gluster_init import (stop_glusterd, start_glusterd,
                                             is_glusterd_running)


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'], ['glusterfs']])
class TestQuorumRelatedMessagesInSyslog(GlusterBaseClass):
    """
    Test Cases in this module related to quorum
    related messages in syslog, when there are more volumes.
    """
    @classmethod
    def setUpClass(cls):
        GlusterBaseClass.setUpClass.im_func(cls)

        # checking for peer status from every node
        ret = cls.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Servers are not in peer probed state")

    def setUp(self):
        """
        setUp method for every test
        """
        # calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)
        self.volume_list = []
        # create a volume
        ret = setup_volume(self.mnode, self.all_servers_info,
                           self.volume)
        self.volume_list.append(self.volname)
        if not ret:
            raise ExecutionError("Volume creation failed: %s" % self.volname)

        # Creating another volume
        second_volume = "second_volume"
        self.volume['name'] = second_volume
        ret = setup_volume(self.mnode, self.all_servers_info,
                           self.volume)
        self.volume_list.append(second_volume)
        if not ret:
            raise ExecutionError("Volume creation failed: %s" % second_volume)

    def tearDown(self):
        """
        tearDown for every test
        """
        if not self.glusterd_service:
            ret = start_glusterd(self.servers[1])
            if not ret:
                raise ExecutionError("Failed to start glusterd services "
                                     "for : %s" % self.servers[1])

        # Checking glusterd service running or not
        ret = is_glusterd_running(self.servers[1])
        if ret == 0:
            g.log.info("glusterd running on :%s", self.servers[1])
        else:
            raise ExecutionError("glusterd not running on :%s"
                                 % self.servers[1])

        # In this test case performing quorum operations,
        # deleting volumes immediately after glusterd services start, volume
        # deletions are failing with quorum not met,
        # that's the reason verifying peers are connected or not before
        # deleting volumes
        peers_not_connected = True
        count = 0
        while count < 10:
            ret = self.validate_peers_are_connected()
            if ret:
                peers_not_connected = False
                break
            count += 1
            sleep(5)
        if peers_not_connected:
            raise ExecutionError("Servers are not in peer probed state")

        # Reverting back the quorum ratio to 51%
        self.quorum_perecent = {'cluster.server-quorum-ratio': '51%'}
        ret = set_volume_options(self.mnode, 'all', self.quorum_perecent)
        if not ret:
            raise ExecutionError(ret, "gluster volume set all cluster"
                                 ".server-quorum- ratio percentage Failed"
                                 " :%s" % self.servers)
        g.log.info("gluster volume set all cluster.server-quorum-ratio 51"
                   "percentage enabled successfully :%s", self.servers)

        # stopping the volume and Cleaning up the volume
        for volume in self.volume_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Failed to Cleanup the "
                                     "Volume %s" % volume)
            g.log.info("Volume deleted successfully : %s", volume)

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_quorum_messages_in_syslog_with_more_volumes(self):
        """
        create two volumes
        Set server quorum to both the volumes
        set server quorum ratio 90%
        stop glusterd service any one of the node
        quorum regain message should be recorded with message id - 106002
        for both the volumes in /var/log/messages and
        /var/log/glusterfs/glusterd.log
        start the glusterd service of same node
        quorum regain message should be recorded with message id - 106003
        for both the volumes in /var/log/messages and
        /var/log/glusterfs/glusterd.log
        """
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-statements

        self.log_messages = "/var/log/messages"
        self.log_glusterd = "/var/log/glusterfs/glusterd.log"

        # Enabling server quorum all volumes
        self.quorum_options = {'cluster.server-quorum-type': 'server'}
        for volume in self.volume_list:
            ret = set_volume_options(self.mnode, volume, self.quorum_options)
            self.assertTrue(ret, "gluster volume set %s cluster.server"
                                 "-quorum-type server Failed" % self.volname)
            g.log.info("gluster volume set %s cluster.server-quorum"
                       "-type server enabled successfully", self.volname)

        # Setting Quorum ratio in percentage
        self.quorum_perecent = {'cluster.server-quorum-ratio': '91%'}
        ret = set_volume_options(self.mnode, 'all', self.quorum_perecent)
        self.assertTrue(ret, "gluster volume set all cluster.server-quorum-"
                             "ratio percentage Failed :%s" % self.servers)
        g.log.info("gluster volume set all cluster.server-quorum-ratio 91 "
                   "percentage enabled successfully :%s", self.servers)

        # counting quorum regain messages-id '106002' in  /var/log/messages
        # file, before glusterd services stop
        cmd_messages = ' '.join(['grep -o', '106002', self.log_messages,
                                 '| wc -l'])
        ret, before_glusterd_stop_msgid_count, _ = g.run(self.mnode,
                                                         cmd_messages)
        self.assertEqual(ret, 0, "Failed to grep quorum regain message-id "
                                 "106002 count in : %s" % self.log_messages)

        # counting quorum regain messages-id '106002' in
        # /var/log/glusterfs/glusterd.log file, before glusterd services stop
        cmd_glusterd = ' '.join(['grep -o', '106002', self.log_glusterd,
                                 '| wc -l'])
        ret, before_glusterd_stop_glusterd_id_count, _ = g.run(self.mnode,
                                                               cmd_glusterd)
        self.assertEqual(ret, 0, "Failed to grep quorum regain message-id "
                                 "106002 count in :%s" % self.log_glusterd)

        # Stopping glusterd services
        ret = stop_glusterd(self.servers[1])
        self.glusterd_service = False
        self.assertTrue(ret, "Failed stop glusterd services : %s"
                        % self.servers[1])
        g.log.info("Stopped glusterd services successfully on: %s",
                   self.servers[1])

        # checking glusterd service stopped or not
        ret = is_glusterd_running(self.servers[1])
        self.assertEqual(ret, 1, "glusterd service should be stopped")

        # counting quorum regain messages-id '106002' in /var/log/messages file
        # after glusterd services stop.
        count = 0
        msg_count = False
        expected_msg_id_count = int(before_glusterd_stop_msgid_count) + 2
        while count <= 10:
            ret, after_glusterd_stop_msgid_count, _ = g.run(self.mnode,
                                                            cmd_messages)
            if(re.search(r'\b' + str(expected_msg_id_count) + r'\b',
                         after_glusterd_stop_msgid_count)):
                msg_count = True
                break
            sleep(5)
            count += 1
        self.assertTrue(msg_count, "Failed to grep quorum regain message-id "
                        "106002 count in :%s" % self.log_messages)

        # counting quorum regain messages-id '106002' in
        # /var/log/glusterfs/glusterd.log file after glusterd services stop
        ret, after_glusterd_stop_glusterd_id_count, _ = g.run(self.mnode,
                                                              cmd_glusterd)
        self.assertEqual(ret, 0, "Failed to grep quorum regain message-id "
                                 "106002 count in :%s" % self.log_glusterd)

        # Finding quorum regain message-id count difference between before
        # and after glusterd services stop in /var/log/messages
        count_diff = (int(after_glusterd_stop_msgid_count) -
                      int(before_glusterd_stop_msgid_count))

        self.assertEqual(count_diff, 2, "Failed to record regain messages "
                                        "in : %s" % self.log_messages)
        g.log.info("regain messages recorded for two volumes "
                   "successfully after glusterd services stop "
                   ":%s", self.log_messages)

        # Finding quorum regain message-id  count difference between before
        # and after glusterd services stop in /var/log/glusterfs/glusterd.log
        count_diff = (int(after_glusterd_stop_glusterd_id_count) -
                      int(before_glusterd_stop_glusterd_id_count))
        self.assertEqual(count_diff, 2, "Failed to record regain messages in "
                                        ": %s" % self.log_glusterd)
        g.log.info("regain messages recorded for two volumes successfully "
                   "after glusterd services stop :%s", self.log_glusterd)

        # counting quorum messages-id '106003' in a /var/log/messages file
        # before glusterd services start
        cmd_messages = ' '.join(['grep -o', '106003', self.log_messages,
                                 '| wc -l'])
        ret, before_glusterd_start_msgid_count, _ = g.run(self.mnode,
                                                          cmd_messages)
        self.assertEqual(ret, 0, "Failed to grep quorum message-id 106003 "
                                 "count in :%s" % self.log_messages)

        # counting quorum regain messages-id '106003' in
        # /var/log/glusterfs/glusterd.log file before glusterd services start
        cmd_glusterd = ' '.join(['grep -o', '106003', self.log_glusterd,
                                 '| wc -l'])
        ret, before_glusterd_start_glusterd_id_count, _ = g.run(self.mnode,
                                                                cmd_glusterd)
        self.assertEqual(ret, 0, "Failed to grep quorum regain message-id "
                                 "106003 count in :%s" % self.log_glusterd)

        # Startin glusterd services
        ret = start_glusterd(self.servers[1])
        self.glusterd_service = True
        self.assertTrue(ret, "Failed to start glusterd "
                             "services: %s" % self.servers[1])

        # Checking glusterd service running or not
        ret = is_glusterd_running(self.servers[1])
        self.assertEqual(ret, 0, "glusterd service should be running")

        # counting quorum messages-id '106003' in a file in a
        # /var/log/messages file after glusterd service start
        count = 0
        expected_msg_id_count = int(before_glusterd_start_msgid_count) + 2
        msg_count = False
        while count <= 10:
            ret, after_glusterd_start_msgid_count, _ = g.run(self.mnode,
                                                             cmd_messages)
            if (re.search(r'\b' + str(expected_msg_id_count) + r'\b',
                          after_glusterd_start_msgid_count)):
                msg_count = True
                break
            sleep(5)
            count += 1

        self.assertTrue(msg_count, "Failed to grep quorum message-id 106003 "
                                   "count in :%s" % self.log_messages)

        # counting quorum regain messages-id '106003' in
        # /var/log/glusterfs/glusterd.log file after glusterd services start
        ret, after_glusterd_start_glusterd_id_count, _ = g.run(self.mnode,
                                                               cmd_glusterd)
        self.assertEqual(ret, 0, "Failed to grep quorum regain message-id "
                                 "106003 count in :%s" % self.log_glusterd)

        # Finding quorum regain message-id count difference between before
        # and after glusterd services start in /var/log/messages
        count_diff = (int(after_glusterd_start_msgid_count) -
                      int(before_glusterd_start_msgid_count))
        self.assertEqual(count_diff, 2, "Failed to record regain "
                                        "messages in :%s" % self.log_messages)
        g.log.info("regain messages recorded for two volumes successfully "
                   "after glusterd services start in :%s", self.log_messages)
        # Finding quorum regain message-id count difference between before
        # and after glusterd services start in /var/log/glusterfs/glusterd.log
        count_diff = (int(after_glusterd_start_glusterd_id_count) -
                      int(before_glusterd_start_glusterd_id_count))
        self.assertEqual(count_diff, 2, "Failed to record regain messages "
                                        "in : %s" % self.log_glusterd)
        g.log.info("regain messages recorded for two volumes successfully "
                   "after glusterd services start :%s", self.log_glusterd)
