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
      Verifying task type and task status in volume status and volume
      status xml
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import get_volume_status, volume_status
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.brick_ops import remove_brick


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestTaskTypeAndStatus(GlusterBaseClass):

    def setUp(self):
        """
        setUp method for every test
        """
        # calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        # Creating Volume
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Volume creation failed: %s" % self.volname)
        g.log.info("Volme created successfully : %s", self.volname)

    def tearDown(self):
        """
        tearDown for every test
        """

        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed Cleanup the Volume %s" % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_status_string(self):
        '''
        -> Create Volume
        -> Start rebalance
        -> Check task type in volume status
        -> Check task status string in volume status
        -> Check task type in volume status xml
        -> Check task status string in volume status xml
        -> Start Remove brick operation
        -> Check task type in volume status
        -> Check task status string in volume status
        -> Check task type in volume status xml
        -> Check task status string in volume status xml
        '''

        # Start rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start rebalance for volume %s"
                         % self.volname)
        g.log.info("Rebalance started successfully on volume %s",
                   self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, "Rebalance failed for volume %s" % self.volname)
        g.log.info("Rebalance completed successfully on volume %s",
                   self.volname)

        # Getting volume status after rebalance start
        ret, out, _ = volume_status(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to get volume status for volume %s"
                         % self.volname)
        g.log.info("Volume status successful on volume %s", self.volname)
        status_list = out.splitlines()

        # Verifying task type from volume status for rebalance
        self.assertIn('Rebalance', status_list[len(status_list) - 4],
                      "Incorrect task type found in volume status for %s"
                      % self.volname)
        g.log.info("Correct task type found in volume status for %s",
                   self.volname)

        # Verifying task status string in volume status for rebalance
        self.assertIn('completed', status_list[len(status_list) - 2],
                      "Incorrect task status found in volume status for %s"
                      % self.volname)
        g.log.info("Correct task status found in volume status for %s",
                   self.volname)

        # Getting volume status --xml after rebalance start
        vol_status = get_volume_status(self.mnode, self.volname,
                                       options='tasks')

        # Verifying task type  from volume status --xml for rebalance
        self.assertEqual('Rebalance',
                         vol_status[self.volname]['task_status'][0]['type'],
                         "Incorrect task type found in volume status xml "
                         "for %s" % self.volname)
        g.log.info("Correct task type found in volume status xml for %s",
                   self.volname)

        # Verifying task status string from volume status --xml for rebalance
        self.assertEqual(
            'completed',
            vol_status[self.volname]['task_status'][0]['statusStr'],
            "Incorrect task status found in volume status "
            "xml for %s" % self.volname)
        g.log.info("Correct task status found in volume status xml %s",
                   self.volname)

        # Getting sub vols
        subvol_dict = get_subvols(self.mnode, self.volname)
        subvol = subvol_dict['volume_subvols'][1]

        # Perform remove brick start
        ret, _, _ = remove_brick(self.mnode, self.volname, subvol,
                                 'start', replica_count=3)
        self.assertEqual(ret, 0, "Failed to start remove brick operation "
                                 "for %s" % self.volname)
        g.log.info("Remove brick operation started successfully on volume %s",
                   self.volname)

        # Getting volume status after remove brick start
        ret, out, _ = volume_status(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to get volume status for volume %s"
                         % self.volname)
        g.log.info("Volume status successful on volume %s", self.volname)
        status_list = out.splitlines()

        # Verifying task type from volume status after remove brick start
        self.assertIn('Remove brick', status_list[len(status_list) - 8],
                      "Incorrect task type found in volume status for "
                      "%s" % self.volname)
        g.log.info("Correct task type found in volume status task for %s",
                   self.volname)

        # Verifying task status string in volume status after remove
        # brick start
        ret = False
        remove_status = ['completed', 'in progress']
        if (status_list[len(status_list) - 2].split(':')[1].strip() in
                remove_status):
            ret = True
        self.assertTrue(ret, "Incorrect task status found in volume status "
                             "task for %s" % self.volname)
        g.log.info("Correct task status found in volume status task for %s",
                   self.volname)

        # Getting volume status --xml after remove brick start
        vol_status = get_volume_status(self.mnode, self.volname,
                                       options='tasks')

        # Verifying task type  from volume status --xml after
        # remove brick start
        self.assertEqual('Remove brick',
                         vol_status[self.volname]['task_status'][0]['type'],
                         "Incorrect task type found in volume status xml for "
                         "%s" % self.volname)
        g.log.info("Correct task type found in volume status xml for %s",
                   self.volname)

        # Verifying task status string from volume status --xml
        # after remove brick start
        ret = False
        if (vol_status[self.volname]['task_status'][0]['statusStr'] in
                remove_status):
            ret = True
        self.assertTrue(ret, "Incorrect task status found in volume status "
                             "xml for %s" % self.volname)
        g.log.info("Correct task status found in volume status xml %s",
                   self.volname)
