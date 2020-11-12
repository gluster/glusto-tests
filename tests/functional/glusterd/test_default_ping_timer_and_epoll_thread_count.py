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

""" Description:
    Tests to check by default ping timer is disabled and epoll
    thread count is 1
"""

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass


class TestPingTimerAndEpollThreadCountDefaultValue(GlusterBaseClass):
    def tearDown(self):
        # Remvoing the test script created during the test
        cmd = "rm -f test.sh;"
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to remove the test script")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_ping_timer_disbaled_and_epoll_thread_count_default_value(self):
        """
        Test Steps:
        1. Start glusterd
        2. Check ping timeout value in glusterd.vol should be 0
        3. Create a test script for epoll thread count
        4. Source the test script
        5. Fetch the pid of glusterd
        6. Check epoll thread count of glusterd should be 1
        """
        # Fetch the ping timeout value from glusterd.vol file
        cmd = "cat /etc/glusterfs/glusterd.vol | grep -i ping-timeout"
        ret, out, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to get ping-timeout value from"
                         " glusterd.vol file")

        # Check if the default value is 0
        self.ping_value = out.split("ping-timeout")
        self.ping_value[1] = (self.ping_value[1]).strip()
        self.assertEqual(int(self.ping_value[1]), 0, "Unexpected: Default"
                         " value of ping-timeout is not 0")

        # Shell Script to be run for epoll thread count
        script = """
                #!/bin/bash
                function nepoll ()
                {
                    local pid=$1;
                    for i in $(ls /proc/$pid/task);
                    do
                        cat /proc/$pid/task/$i/stack | grep epoll_wait;
                    done
                }
                """

        # Execute the shell script
        cmd = "echo '{}' > test.sh;".format(script)
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to create the file with the script")

        # Fetch the pid of glusterd
        cmd = "pidof glusterd"
        ret, pidof_glusterd, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to get the pid of glusterd")
        pidof_glusterd = int(pidof_glusterd)

        # Check the epoll thread count of glusterd
        cmd = "source test.sh; nepoll %d | wc -l" % pidof_glusterd
        ret, count, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to get the epoll thread count")
        self.assertEqual(int(count), 1, "Unexpected: Default epoll thread"
                         "count is not 1")
