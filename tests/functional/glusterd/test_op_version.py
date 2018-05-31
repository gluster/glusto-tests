#  Copyright (C) 2018  Red Hat, Inc. <http://www.redhat.com>
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
      Test for setting up the max supported op-version and
      verifying  version number in info file
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (get_volume_options,
                                           set_volume_options)


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestMaxSupportedOpVersion(GlusterBaseClass):

    def setUp(self):
        """
        setUp method for every test
        """
        # calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        # Creating Volume
        g.log.info("Started creating volume")
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Volume creation failed: %s" % self.volname)

    def tearDown(self):
        """
        tearDown for every test
        """
        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed Cleanup the Volume %s" % self.volname)

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_op_version(self):
        '''
        -> Create Volume
        -> Get the current op-version
        -> Get the max supported op-version
        -> Verify vol info file exists or not in all servers
        -> Get the version number from vol info file
        -> If current op-version is less than max-op-version
        set the current op-version to max-op-version
        -> After vol set operation verify that version number
        increased by one or not in vol info file
        -> verify that current-op-version and max-op-version same or not.
        '''

        # Getting current op-version
        vol_dict = get_volume_options(self.mnode, 'all',
                                      'cluster.op-version')
        current_op_version = int(vol_dict['cluster.op-version'])

        # Getting Max op-verison
        all_dict = get_volume_options(self.mnode, 'all')
        max_op_version = int(all_dict['cluster.max-op-version'])

        # File_path: path for vol info file
        # Checking vol file exist in all servers or not
        file_path = '/var/lib/glusterd/vols/' + self.volname + '/info'
        for server in self.servers:
            conn = g.rpyc_get_connection(server)
            ret = conn.modules.os.path.isfile(file_path)
            self.assertTrue(ret, "Vol file not found in server %s" % server)
            g.log.info("vol file found in server %s", server)
        g.rpyc_close_deployed_servers()

        # Getting version number from vol info file
        # cmd: grepping  version from vol info file
        ret, out, _ = g.run(self.mnode,
                            ' '.join(['grep', "'^version'", file_path]))
        version_list = out.split('=')
        version_no = int(version_list[1]) + 1

        # Comparing current op-version and max op-version
        if current_op_version < max_op_version:

            # Set max-op-version
            ret = set_volume_options(self.mnode, 'all',
                                     {'cluster.op-version': max_op_version})
            self.assertTrue(ret, "Failed to set max op-version for cluster")
            g.log.info("Setting up max-op-version is successful for cluster")

            # Grepping version number from vol info file after
            # vol set operation
            ret, out, _ = g.run(self.mnode,
                                ' '.join(['grep', "'^version'", file_path]))
            version_list = out.split('=')
            after_version_no = int(version_list[1])

            # Comparing version number before and after vol set operations
            self.assertEqual(version_no, after_version_no,
                             "After volume set operation version "
                             "number not increased by one")
            g.log.info("After volume set operation version number "
                       "increased by one")

            # Getting current op-version
            vol_dict = get_volume_options(self.mnode, 'all',
                                          'cluster.op-version')
            current_op_version = int(vol_dict['cluster.op-version'])

        # Checking current-op-version and max-op-version equal or not
        self.assertEqual(current_op_version, max_op_version,
                         "Current op-version and max op-version "
                         "are not same")
        g.log.info("current-op-version and max-op-version of cluster are same")
