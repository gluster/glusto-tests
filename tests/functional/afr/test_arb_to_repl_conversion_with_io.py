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

from datetime import datetime, timedelta
from time import sleep, time

from glusto.core import Glusto as g

from glustolibs.gluster.brick_ops import add_brick, remove_brick
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.heal_libs import monitor_heal_completion
from glustolibs.gluster.heal_ops import trigger_heal
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.volume_ops import get_volume_info, set_volume_options
from glustolibs.io.utils import (collect_mounts_arequal, validate_io_procs,
                                 wait_for_io_to_complete)
from glustolibs.misc.misc_libs import upload_scripts


# pylint: disable=too-many-locals,too-many-statements
@runs_on([['arbiter', 'distributed-arbiter'], ['glusterfs']])
class TestArbiterToReplicatedConversion(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()
        cls.script_path = '/usr/share/glustolibs/io/scripts/file_dir_ops.py'
        ret = upload_scripts(cls.clients, cls.script_path)
        if not ret:
            raise ExecutionError('Failed to upload IO scripts to clients')

    def setUp(self):
        self.get_super_method(self, 'setUp')()
        self.all_mounts_procs = []
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError('Failed to setup and mount '
                                 '{}'.format(self.volname))

    def tearDown(self):
        if self.all_mounts_procs:
            ret = wait_for_io_to_complete(self.all_mounts_procs,
                                          [self.mounts[1]])
            if not ret:
                raise ExecutionError('Wait for IO completion failed on client')
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError('Not able to unmount and cleanup '
                                 '{}'.format(self.volname))
        self.get_super_method(self, 'tearDown')()

    def _get_arbiter_bricks(self):
        """
        Returns tuple of arbiter bricks from the volume
        """

        # Get all subvols
        subvols = get_subvols(self.mnode, self.volname)
        self.assertTrue(subvols,
                        'Not able to get subvols of {}'.format(self.volname))

        # Last brick in every subvol will be the arbiter
        return tuple(zip(*subvols.get('volume_subvols')))[-1]

    def test_arb_to_repl_conversion_with_io(self):
        """
        Description: To perform a volume conversion from Arbiter to Replicated
        with background IOs

        Steps:
        - Create, start and mount an arbiter volume in two clients
        - Create two dir's, fill IO in first dir and take note of arequal
        - Start a continuous IO from second directory
        - Convert arbiter to x2 replicated volume (remove brick)
        - Convert x2 replicated to x3 replicated volume (add brick)
        - Wait for ~5 min for vol file to be updated on all clients
        - Enable client side heal options and issue volume heal
        - Validate heal completes with no errors and arequal of first dir
          matches against initial checksum
        """

        client, m_point = (self.mounts[0].client_system,
                           self.mounts[0].mountpoint)

        # Fill IO in first directory
        cmd = ('/usr/bin/env python {} '
               'create_deep_dirs_with_files --dir-depth 10 '
               '--fixed-file-size 1M --num-of-files 100 '
               '--dirname-start-num 1 {}'.format(self.script_path, m_point))
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, 'Not able to fill directory with IO')

        # Take `arequal` checksum on first directory
        ret, exp_arequal = collect_mounts_arequal(self.mounts[0],
                                                  m_point + '/user1')
        self.assertTrue(ret, 'Failed to get arequal checksum on mount')

        # Start continuous IO from second directory
        client = self.mounts[1].client_system
        cmd = ('/usr/bin/env python {} '
               'create_deep_dirs_with_files --dir-depth 10 '
               '--fixed-file-size 1M --num-of-files 250 '
               '--dirname-start-num 2 {}'.format(self.script_path, m_point))
        proc = g.run_async(client, cmd)
        self.all_mounts_procs.append(proc)

        # Wait for IO to fill before volume conversion
        sleep(30)

        # Remove arbiter bricks ( arbiter to x2 replicated )
        kwargs = {'replica_count': 2}
        ret, _, _ = remove_brick(self.mnode,
                                 self.volname,
                                 self._get_arbiter_bricks(),
                                 option='force',
                                 **kwargs)
        self.assertEqual(ret, 0, 'Not able convert arbiter to x2 replicated '
                         'volume')
        # Wait for IO to fill after volume conversion
        sleep(30)

        # Add bricks (x2 replicated to x3 replicated)
        kwargs['replica_count'] = 3
        vol_info = get_volume_info(self.mnode, volname=self.volname)
        self.assertIsNotNone(vol_info, 'Not able to get volume info')
        dist_count = vol_info[self.volname]['distCount']
        bricks_list = form_bricks_list(
            self.mnode,
            self.volname,
            number_of_bricks=int(dist_count) * 1,
            servers=self.servers,
            servers_info=self.all_servers_info,
        )
        self.assertTrue(bricks_list, 'Not able to get unused list of bricks')
        ret, _, _ = add_brick(self.mnode,
                              self.volname,
                              bricks_list,
                              force='True',
                              **kwargs)
        self.assertEqual(ret, 0, 'Not able to add-brick to '
                         '{}'.format(self.volname))
        # Wait for IO post x3 replicated volume conversion
        sleep(30)

        # Validate volume info
        vol_info = get_volume_info(self.mnode, volname=self.volname)
        self.assertIsNotNone(vol_info, 'Not able to get volume info')
        vol_info = vol_info[self.volname]
        repl_count, brick_count = (vol_info['replicaCount'],
                                   vol_info['brickCount'])

        # Wait for the volfile to sync up on clients
        cmd = ('grep -ir connected {}/.meta/graphs/active/{}-client-*/private '
               '| wc -l')
        wait_time = time() + 300
        in_sync = False
        while time() <= wait_time:
            ret, rout, _ = g.run(client, cmd.format(m_point, self.volname))
            self.assertEqual(ret, 0,
                             'Not able to grep for volfile sync from client')
            if int(rout) == int(brick_count):
                in_sync = True
                break
            sleep(30)
        self.assertTrue(
            in_sync, 'Volfiles from clients are not synced even '
            'after polling for ~5 min')

        self.assertEqual(
            int(repl_count), kwargs['replica_count'], 'Not able '
            'to validate x2 to x3 replicated volume conversion')

        # Enable client side heal options, trigger and monitor heal
        ret = set_volume_options(
            self.mnode, self.volname, {
                'data-self-heal': 'on',
                'entry-self-heal': 'on',
                'metadata-self-heal': 'on'
            })
        self.assertTrue(ret, 'Unable to set client side heal options')
        ret = trigger_heal(self.mnode, self.volname)
        self.assertTrue(ret, 'Unable to trigger heal on volume')
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret,
                        'Heal is not completed for {}'.format(self.volname))

        # Validate IO
        prev_time = datetime.now().replace(microsecond=0)
        ret = validate_io_procs(self.all_mounts_procs, [self.mounts[1]])
        curr_time = datetime.now().replace(microsecond=0)
        self.assertTrue(ret, 'Not able to validate completion of IO on mount')
        self.all_mounts_procs *= 0

        # To ascertain IO was happening during brick operations
        self.assertGreater(
            curr_time - prev_time, timedelta(seconds=10), 'Unable '
            'to validate IO was happening during brick operations')

        # Take and validate `arequal` checksum on first directory
        ret, act_areequal = collect_mounts_arequal(self.mounts[1],
                                                   m_point + '/user1')
        self.assertTrue(ret, 'Failed to get arequal checksum from mount')
        self.assertEqual(
            exp_arequal, act_areequal, '`arequal` checksum did '
            'not match post arbiter to x3 replicated volume conversion')

        g.log.info('PASS: Arbiter to x3 replicated volume conversion complete')
