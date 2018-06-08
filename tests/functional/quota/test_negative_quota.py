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

import uuid
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_set_soft_timeout,
                                          quota_set_hard_timeout)
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'], ['glusterfs']])
class TestNegativeQuota(GlusterBaseClass):
    """This testcase will enable/disable quota by giving negative inputs
    and also try to enable timeouts by giving huge value , all testcases
    have to return false
    """

    def setUp(self):
        """ creates the volume and mount it"""
        GlusterBaseClass.setUp.im_func(self)
        g.log.info("Creating the vol %s and mount it", self.volname)
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Volume %s creation "
                                 "failed" % (self.volname))
        g.log.info("Successfully created volume %s", self.volname)

    def tearDown(self):
        """ clean volume and unmount it """
        g.log.info("starting to unmount and clean volume %s", self.volname)
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to delete the "
                                 "volume" % (self.volname))
        GlusterBaseClass.tearDown.im_func(self)

    def test_negative_quota_enable(self):
        """ This testcase will enable quota by giving negative inputs or
        by missing keywords, all cases have to return false.
        """

        # give typo err cmd
        cmd = "gluster volume quote %s enable" % (self.volname)
        ret, _, err = g.run(self.mnode, cmd)
        self.assertEqual(ret, 1, "Unexpected: Quota is enabled "
                         "with typo err cmd")

        # try typo err cmd again
        cmd = "gluster volume quota %s enablee" % (self.volname)
        ret, _, err = g.run(self.mnode, cmd)
        self.assertEqual(ret, 1, "Unexpected: Quota is enabled "
                         "with typo err cmd")

        # try missing enable key word now
        cmd = "gluster volume quota %s" % (self.volname)
        ret, _, err = g.run(self.mnode, cmd)
        self.assertEqual(ret, 1, "Unexpected: Quota is enabled "
                         "with missing keyword")

        # try wrong volname
        random_name = str(uuid.uuid4()).split('-')[0]
        cmd = "gluster volume quota %s enable" % (random_name)
        g.log.info("running %s", cmd)
        ret, _, err = g.run(self.mnode, cmd)
        errmsg = ("quota command failed : Volume %s "
                  "does not exist\n" % (random_name))
        msg = "expected %s, but returned %s" % (errmsg, err)
        self.assertEqual(err, errmsg, msg)

    def test_negative_quota_disable(self):
        """ This testcase will try to disable quota by giving
        wrong keywords and missing volume name, all cases has
        to return false
        """

        g.log.info("enabling quota for %s volume", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Error in enabling quota")

        # test to disable quota by spell mistake
        cmd = "gluster volume quote %s disablee" % (self.volname)
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 1, "Unexpected: Quota is disabled "
                         "with typo err cmd")

        # test to disable quota again by missing volname
        cmd = "gluster volume quota disable"
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 1, "Unexpected: Quota is disabled "
                         "without giving volname")

        # test to disable quota by missing keyword
        random_name = str(uuid.uuid4()).split('-')[0]
        cmd = ("gluster volume quota %s", random_name)
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 1, "Unexpected: Quota is disabled "
                         "even with missing keyword")

    def test_negative_quota_timeouts(self):
        """ This testcase try to enable soft/hard timeouts by giving
        huge value , all cases has to return false
        """
        ret, _, err = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Error in enabling quota for %s" %
                         (self.volname))

        # now try to enable timeout with more time
        time_in_secs = 100 * 60 * 60
        g.log.info("Setting up soft timeout with %d secs", time_in_secs)
        ret, _, err = quota_set_soft_timeout(self.mnode,
                                             self.volname,
                                             str(time_in_secs))
        errmsg = ("quota command failed : '%d' in "
                  "'option soft-timeout %d' is out "
                  "of range [0 - 1800]\n" % (time_in_secs, time_in_secs))
        self.assertEqual(err, errmsg, "expected %s but returned %s" %
                         (errmsg, err))

        # now try to enable hard timeout with more time
        g.log.info("Setting up hard timeout with %d secs", time_in_secs)
        ret, _, err = quota_set_hard_timeout(self.mnode,
                                             self.volname,
                                             str(time_in_secs))
        errmsg = ("quota command failed : '%d' in "
                  "'option hard-timeout %d' is "
                  "out of range [0 - 60]\n" % (time_in_secs, time_in_secs))
        self.assertEqual(err, errmsg, "expected %s but returned %s" %
                         (errmsg, err))
