#  Copyright (C) 2016-2017  Red Hat, Inc. <http://www.redhat.com>
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

import random
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import cleanup_volume
from glustolibs.gluster.volume_ops import (get_volume_list, volume_create)
from glustolibs.gluster.lib_utils import (form_bricks_list,
                                          is_core_file_created)


@runs_on([['distributed'], ['glusterfs']])
class TestConcurrentSet(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        GlusterBaseClass.setUpClass.im_func(cls)
        g.log.info("Starting %s ", cls.__name__)
        ret = cls.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Nodes are not in peer probe state")

    def tearDown(self):
        '''
        clean up all volumes and detaches peers from cluster
        '''
        vol_list = get_volume_list(self.mnode)
        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            self.assertTrue(ret, "Failed to Cleanup the Volume %s" % volume)
            g.log.info("Volume deleted successfully : %s", volume)

        GlusterBaseClass.tearDown.im_func(self)

    def test_concurrent_set(self):
        # time stamp of current test case
        ret, test_timestamp, _ = g.run_local('date +%s')
        test_timestamp = test_timestamp.strip()
        # Create a volume
        self.volname = "first-vol"
        self.brick_list = form_bricks_list(self.mnode, self.volname, 3,
                                           self.servers,
                                           self.all_servers_info)

        ret = volume_create(self.mnode, self.volname,
                            self.brick_list, force=False)
        self.assertEqual(ret[0], 0, ("Unable"
                                     "to create volume %s" % self.volname))
        g.log.info("Volume created successfuly %s", self.volname)

        # Create a volume
        self.volname = "second-vol"
        self.brick_list = form_bricks_list(self.mnode, self.volname, 3,
                                           self.servers,
                                           self.all_servers_info)
        g.log.info("Creating a volume")
        ret = volume_create(self.mnode, self.volname,
                            self.brick_list, force=False)
        self.assertEqual(ret[0], 0, ("Unable"
                                     "to create volume %s" % self.volname))
        g.log.info("Volume created successfuly %s", self.volname)

        cmd1 = ("for i in `seq 1 100`; do gluster volume set first-vol "
                "read-ahead on; done")
        cmd2 = ("for i in `seq 1 100`; do gluster volume set second-vol "
                "write-behind on; done")

        proc1 = g.run_async(random.choice(self.servers), cmd1)
        proc2 = g.run_async(random.choice(self.servers), cmd2)

        ret1, _, _ = proc1.async_communicate()
        ret2, _, _ = proc2.async_communicate()

        self.assertEqual(ret1, 0, "Concurrent volume set on different volumes "
                         "simultaneously failed")
        self.assertEqual(ret2, 0, "Concurrent volume set on different volumes "
                         "simultaneously failed")

        g.log.info("Setting options on different volumes @ same time "
                   "successfully completed")
        ret = is_core_file_created(self.servers, test_timestamp)
        if ret:
            g.log.info("No core file found, glusterd service "
                       "running successfully")
        else:
            g.log.error("core file found in directory, it "
                        "indicates the glusterd service crash")
            self.assertTrue(ret, ("glusterd service should not crash"))
