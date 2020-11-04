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


import time
import itertools
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.volume_ops import (set_volume_options,
                                           get_volume_options)


@runs_on([['dispersed', 'distributed-dispersed'], ['glusterfs']])
class EcVerifyLock(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()
        cls.script = "/usr/share/glustolibs/io/scripts/file_lock.py"
        if not upload_scripts(cls.clients, [cls.script]):
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        # Setup Volume and Mount Volume
        if not self.setup_volume_and_mount_volume(mounts=self.mounts):
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def test_verify_lock_granted_from_2_clients(self):
        """
        - Create disperse volume and mount it to 2 clients`
        - Create file from 1 client on mount point
        - Take lock from client 1 => Lock is acquired
        - Try taking lock from client 2=> Lock is blocked (as already
          being taken by client 1)
        - Release lock from client1=> Lock is released
        - Take lock from client2
        - Again try taking lock from client 1
        - verify test with once, by disabling eagerlock and other eager lock
          and once by leaving eager and other eagerlock enabled(by default)
        """
        mpoint = self.mounts[0].mountpoint

        # Create a file on client 1
        cmd = "touch {}/test_file".format(mpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to create file on client 1")

        # Verifying OCL as ON
        option = "optimistic-change-log"
        option_dict = get_volume_options(self.mnode, self.volname, option)
        self.assertIsNotNone(option_dict, ("Failed to get %s volume option"
                                           " for volume %s"
                                           % (option, self.volname)))
        self.assertEqual(option_dict['disperse.optimistic-change-log'], 'on',
                         ("%s is not ON for volume %s" % (option,
                                                          self.volname)))
        g.log.info("Succesfully verified %s value for volume %s",
                   option, self.volname)

        # Repeat the test with eager-lock and other-eager-lock 'on' & 'off'
        for lock_status in ('on', 'off'):
            options = {'disperse.eager-lock': lock_status,
                       'disperse.other-eager-lock': lock_status}
            ret = set_volume_options(self.mnode, self.volname, options)

            self.assertTrue(ret, ("failed to set eagerlock and other "
                                  "eagerlock value as %s " % lock_status))
            g.log.info("Successfully set eagerlock and other eagerlock value"
                       " to %s", lock_status)

            # Repeat the test for both the combinations of clients
            for client_1, client_2 in list(itertools.permutations(
                    [self.mounts[0].client_system,
                     self.mounts[1].client_system], r=2)):
                # Get lock to file from one client
                lock_cmd = ("/usr/bin/env python {} -f {}/"
                            "test_file -t 30".format(self.script, mpoint))
                proc = g.run_async(client_1, lock_cmd)
                time.sleep(5)

                # As the lock is been acquired by one client,
                # try to get lock from the other
                ret, _, _ = g.run(client_2, lock_cmd)
                self.assertEqual(ret, 1, ("Unexpected: {} acquired the lock "
                                          "before been released by {}"
                                          .format(client_2, client_1)))
                g.log.info("Expected : Lock can't be acquired by %s before "
                           "being released by %s", client_2, client_1)

                # Wait for first client to release the lock.
                ret, _, _ = proc.async_communicate()
                self.assertEqual(ret, 0, ("File lock process failed on %s:%s",
                                          client_1, mpoint))

                # Try taking the lock from other client and releasing it
                lock_cmd = ("/usr/bin/env python {} -f "
                            "{}/test_file -t 1".format(self.script, mpoint))
                ret, _, _ = g.run(client_2, lock_cmd)
                self.assertEqual(ret, 0,
                                 ("Unexpected:{} Can't acquire the lock even "
                                  "after its been released by {}"
                                  .format(client_2, client_1)))
                g.log.info("Successful, Lock acquired by %s after being "
                           "released by %s", client_2, client_1)

    def tearDown(self):
        # Stopping the volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup "
                                 "Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
