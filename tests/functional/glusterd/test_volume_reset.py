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

""" Description:
"""
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import cleanup_volume
from glustolibs.gluster.bitrot_ops import (enable_bitrot, is_bitd_running,
                                           is_scrub_process_running)
from glustolibs.gluster.uss_ops import enable_uss, is_snapd_running


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'], ['glusterfs']])
class GlusterdVolumeReset(GlusterBaseClass):
    '''
    Test Cases in this module related to Glusterd volume reset validation
    with bitd, scrub and snapd daemons running or not
    '''
    @classmethod
    def setUpClass(cls):
        GlusterBaseClass.setUpClass.im_func(cls)
        g.log.info("Starting %s ", cls.__name__)

        # Creating Volume
        g.log.info("Started creating volume")
        ret = cls.setup_volume()
        if ret:
            g.log.info("Volme created successfully : %s", cls.volname)
        else:
            raise ExecutionError("Volume creation failed: %s" % cls.volname)

    def setUp(self):
        """
        setUp method for every test
        """
        # calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        # command for volume reset
        g.log.info("started resetting volume")
        cmd = "gluster volume reset " + self.volname
        ret, _, _ = g.run(self.mnode, cmd)
        if ret == 0:
            g.log.info("volume reset successfully :%s", self.volname)
        else:
            raise ExecutionError("Volume reset Failed :%s" % self.volname)

    def tearDown(self):
        """
        tearDown for every test
        """
        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    @classmethod
    def tearDownClass(cls):
        # stopping the volume and Cleaning up the volume
        ret = cleanup_volume(cls.mnode, cls.volname)
        if ret:
            g.log.info("Volume deleted successfully : %s", cls.volname)
        else:
            raise ExecutionError("Failed Cleanup the Volume %s" % cls.volname)

    def test_bitd_scrubd_snapd_after_volume_reset(self):
        '''
        -> Create volume
        -> Enable BitD, Scrub and Uss on volume
        -> Verify  the BitD, Scrub and Uss  daemons are running on every node
        -> Reset the volume
        -> Verify the Daemons (BitD, Scrub & Uss ) are running or not
        -> Eanble Uss on same volume
        -> Reset the volume with force
        -> Verify all the daemons(BitD, Scrub & Uss) are running or not
        '''

        # enable bitrot and scrub on volume
        g.log.info("Enabling bitrot")
        ret, _, _ = enable_bitrot(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to enable bitrot on volume: %s" %
                         self.volname)
        g.log.info("Bitd and scrub daemons enabled successfully on volume :%s",
                   self.volname)

        # enable uss on volume
        g.log.info("Enabling snaphot(uss)")
        ret, _, _ = enable_uss(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to enable uss on volume: %s" %
                         self.volname)
        g.log.info("uss enabled successfully on  volume :%s", self.volname)

        # Checks bitd, snapd, scrub daemons running or not
        g.log.info("checking snapshot, scrub and bitrot\
        daemons running or not")
        for mnode in self.servers:
            ret = is_bitd_running(mnode, self.volname)
            self.assertTrue(ret, "Bitrot Daemon not running on %s server:"
                            % mnode)
            ret = is_scrub_process_running(mnode, self.volname)
            self.assertTrue(ret, "Scrub Daemon not running on %s server:"
                            % mnode)
            ret = is_snapd_running(mnode, self.volname)
            self.assertTrue(ret, "Snap Daemon not running %s server:" % mnode)
        g.log.info("bitd, scrub and snapd running successflly on volume :%s",
                   self.volname)

        # command for volume reset
        g.log.info("started resetting volume")
        cmd = "gluster volume reset " + self.volname
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "volume reset failed for : %s" % self.volname)
        g.log.info("volume resetted succefully :%s", self.volname)

        # After volume reset snap daemon will not be running,
        # bitd and scrub deamons will be in running state.
        g.log.info("checking snapshot, scrub and bitrot daemons\
        running or not after volume reset")
        for mnode in self.servers:
            ret = is_bitd_running(mnode, self.volname)
            self.assertTrue(ret, "Bitrot Daemon\
            not running on %s server:" % mnode)
            ret = is_scrub_process_running(mnode, self.volname)
            self.assertTrue(ret, "Scrub Daemon\
            not running on %s server:" % mnode)
            ret = is_snapd_running(mnode, self.volname)
            self.assertFalse(ret, "Snap Daemon should not be running on %s "
                             "server after volume reset:" % mnode)
        g.log.info("bitd and scrub daemons are running after volume reset "
                   "snapd is not running as expected on volume :%s",
                   self.volname)

        # enable uss on volume
        g.log.info("Enabling snaphot(uss)")
        ret, _, _ = enable_uss(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to enable uss on volume: %s" %
                         self.volname)
        g.log.info("uss enabled successfully on volume :%s", self.volname)

        # command for volume reset with force
        g.log.info("started resetting volume with force option")
        cmd = "gluster volume reset " + self.volname + " force"
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "volume reset fail\
               for : %s" % self.volname)
        g.log.info("Volume reset sucessfully with force option :%s",
                   self.volname)

        # After volume reset bitd, snapd, scrub daemons will not be running,
        # all three daemons will get die
        g.log.info("checking snapshot, scrub and bitrot daemons\
        running or not after volume reset with force")
        for mnode in self.servers:
            ret = is_bitd_running(mnode, self.volname)
            self.assertFalse(ret, "Bitrot Daemon should not be\
            running on %s server after volume reset with force:" % mnode)
            ret = is_scrub_process_running(mnode, self.volname)
            self.assertFalse(ret, "Scrub Daemon shiuld not be running\
            on %s server after volume reset with force:" % mnode)
            ret = is_snapd_running(mnode, self.volname)
            self.assertFalse(ret, "Snap Daemon should not be\
            running on %s server after volume reset force:" % mnode)
        g.log.info("After volume reset bitd, scrub and snapd are not running "
                   "after volume reset with force on volume :%s", self.volname)
