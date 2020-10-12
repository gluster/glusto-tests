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

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed',
           'arbiter', 'distributed-arbiter'], ['glusterfs']])
class TestFlock(GlusterBaseClass):
    def setUp(self):
        """
        setUp method for every test
        """
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Volume creation failed: %s" % self.volname)

    def tearDown(self):
        """
        TearDown for every test
        """
        # Stopping the volume and Cleaning up the volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError(
                "Failed Cleanup the Volume %s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    def test_gluster_does_not_do_posix_lock_when_multiple_client(self):
        """
        Steps:
        1. Create all types of volumes.
        2. Mount the brick on two client mounts
        3. Prepare same script to do flock on the two nodes
         while running this script it should not hang
        4. Wait till 300 iteration on both the node
        """

        # Shell Script to be run on mount point
        script = """
                #!/bin/bash
                flock_func(){
                file=/bricks/brick0/test.log
                touch $file
                (
                         flock -xo 200
                         echo "client1 do something" > $file
                         sleep 1
                 ) 300>$file
                }
                i=1
                while [ "1" = "1" ]
                do
                    flock_func
                    ((i=i+1))
                    echo $i
                    if [[ $i == 300 ]]; then
                            break
                    fi
                done
                """
        mount_point = self.mounts[0].mountpoint
        cmd = "echo '{}' >'{}'/test.sh; sh '{}'/test.sh ".format(
            script, mount_point, mount_point)
        ret = g.run_parallel(self.clients[:2], cmd)

        # Check if 300 is present in the output
        for client_ip, _ in ret.items():
            self.assertTrue("300" in ret[client_ip][1].split("\n"),
                            "300 iteration is not completed")
            self.assertFalse(ret[client_ip][0], "Failed to run the cmd ")
