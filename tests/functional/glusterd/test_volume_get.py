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

"""
Test Cases in this module related to Gluster volume get functionality
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (get_volume_options,
                                           set_volume_options)
from glustolibs.gluster.lib_utils import is_core_file_created


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'], ['glusterfs']])
class TestVolumeGet(GlusterBaseClass):
    def setUp(self):
        """
        setUp method for every test
        """
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

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
        self.get_super_method(self, 'tearDown')()

    def test_volume_get(self):
        """
        desc: performing different combinations of gluter
        volume get functionalities
        1. Create a gluster cluster
        2. Get the option from the non-existing volume,
        gluster volume get <non-existing vol> io-cache
        3. Get all options from the non-existing volume,
        gluster volume get <non-existing volume > all
        4. Provide a incorrect command syntax to get the options
        from the volume
            gluster volume get <vol-name>
            gluster volume get
            gluster volume get io-cache
        5. Create any type of volume in the cluster
        6. Get the value of the non-existing option
            gluster volume get <vol-name> temp.key
        7. get all options set on the volume
            gluster volume get <vol-name> all
        8. get the specific option set on the volume
            gluster volume get <vol-name> io-cache
        9. Set an option on the volume
            gluster volume set <vol-name> performance.low-prio-threads 14
        10. Get all the options set on the volume and check
        for low-prio-threads
            gluster volume get <vol-name> all then get the
            low-prio-threads value
        11. Get all the options set on the volume
                gluster volume get <vol-name> all
        12.  Check for any cores in "cd /"
        """
        # pylint: disable=too-many-statements

        # time stamp of current test case
        ret, test_timestamp, _ = g.run_local('date +%s')
        test_timestamp = test_timestamp.strip()

        # performing gluster volume get command for non exist volume io-cache
        self.non_exist_volume = "abc99"
        ret, _, err = g.run(self.mnode, "gluster volume get %s io-cache"
                            % self.non_exist_volume)
        self.assertNotEqual(ret, 0, "gluster volume get command should fail "
                                    "for non existing volume with io-cache "
                                    "option :%s" % self.non_exist_volume)
        msg = ('Volume ' + self.non_exist_volume + ' does not exist')
        self.assertIn(msg, err, "No proper error message for non existing "
                                "volume with io-cache option :%s"
                      % self.non_exist_volume)
        g.log.info("gluster volume get command failed successfully for non "
                   "existing volume with io-cache option"
                   ":%s", self.non_exist_volume)

        # performing gluster volume get all command for non exist volume
        ret, _, err = g.run(self.mnode, "gluster volume get %s all" %
                            self.non_exist_volume)
        self.assertNotEqual(ret, 0, "gluster volume get command should fail "
                                    "for non existing volume %s with all "
                                    "option" % self.non_exist_volume)
        self.assertIn(msg, err, "No proper error message for non existing "
                                "volume with all option:%s"
                      % self.non_exist_volume)
        g.log.info("gluster volume get command failed successfully for non "
                   "existing volume with all option :%s",
                   self.non_exist_volume)

        # performing gluster volume get command for non exist volume
        ret, _, err = g.run(self.mnode, "gluster volume get "
                            "%s" % self.non_exist_volume)
        self.assertNotEqual(ret, 0, "gluster volume get command should "
                                    "fail for non existing volume :%s"
                            % self.non_exist_volume)
        msg = 'get <VOLNAME|all> <key|all>'
        self.assertIn(msg, err, "No proper error message for non existing "
                                "volume :%s" % self.non_exist_volume)
        g.log.info("gluster volume get command failed successfully for non "
                   "existing volume :%s", self.non_exist_volume)

        # performing gluster volume get command without any volume name given
        ret, _, err = g.run(self.mnode, "gluster volume get")
        self.assertNotEqual(ret, 0, "gluster volume get command should fail")
        self.assertIn(msg, err, "No proper error message for gluster "
                                "volume get command")
        g.log.info("gluster volume get command failed successfully")

        # performing gluster volume get io-cache command
        # without any volume name given
        ret, _, err = g.run(self.mnode, "gluster volume get io-cache")
        self.assertNotEqual(ret, 0, "gluster volume get io-cache command "
                                    "should fail")
        self.assertIn(msg, err, "No proper error message for gluster volume "
                                "get io-cache command")
        g.log.info("gluster volume get io-cache command failed successfully")

        # gluster volume get volname with non existing option
        ret, _, err = g.run(self.mnode, "gluster volume "
                                        "get %s temp.key" % self.volname)
        self.assertNotEqual(ret, 0, "gluster volume get command should fail "
                                    "for existing volume %s with non-existing "
                                    "option" % self.volname)
        msg = 'Did you mean auth.allow or ...reject?'
        if msg not in err:
            msg = 'volume get option: failed: Did you mean ctime.noatime?'
        self.assertIn(msg, err, "No proper error message for existing "
                                "volume %s with non-existing option"
                      % self.volname)
        g.log.info("gluster volume get command failed successfully for "
                   "existing volume %s with non existing option",
                   self.volname)

        # performing gluster volume get volname all

        ret = get_volume_options(self.mnode, self.volname, "all")
        self.assertIsNotNone(ret, "gluster volume get %s all command "
                                  "failed" % self.volname)
        g.log.info("gluster volume get %s all command executed "
                   "successfully", self.volname)

        # performing gluster volume get volname io-cache
        ret = get_volume_options(self.mnode, self.volname, "io-cache")
        self.assertIsNotNone(ret, "gluster volume get %s io-cache command "
                                  "failed" % self.volname)
        self.assertIn("on", ret['performance.io-cache'], "io-cache value "
                                                         "is not correct")
        g.log.info("io-cache value is correct")

        # Performing gluster volume set volname performance.low-prio-threads
        prio_thread = {'performance.low-prio-threads': '14'}
        ret = set_volume_options(self.mnode, self.volname, prio_thread)
        self.assertTrue(ret, "gluster volume set %s performance.low-prio-"
                             "threads failed" % self.volname)
        g.log.info("gluster volume set %s "
                   "performance.low-prio-threads executed successfully",
                   self.volname)

        # Performing gluster volume get all, checking low-prio threads value
        ret = get_volume_options(self.mnode, self.volname, "all")
        self.assertIsNotNone(ret, "gluster volume get %s all "
                                  "failed" % self.volname)
        self.assertIn("14", ret['performance.low-prio-threads'],
                      "performance.low-prio-threads value is not correct")
        g.log.info("performance.low-prio-threads value is correct")

        # performing gluster volume get volname all
        ret = get_volume_options(self.mnode, self.volname, "all")
        self.assertIsNotNone(ret, "gluster volume get %s all command "
                                  "failed" % self.volname)
        g.log.info("gluster volume get %s all command executed "
                   "successfully", self.volname)

        # Checking core file created or not in "/" directory
        ret = is_core_file_created(self.servers, test_timestamp)
        self.assertTrue(ret, "glusterd service should not crash")
        g.log.info("No core file found, glusterd service "
                   "running successfully")
