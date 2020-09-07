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

from random import choice
from time import sleep, time

from glusto.core import Glusto as g

from glustolibs.gluster.brick_libs import bring_bricks_offline
from glustolibs.gluster.dht_test_utils import find_hashed_subvol
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.heal_libs import monitor_heal_completion
from glustolibs.gluster.heal_ops import heal_info
from glustolibs.gluster.volume_libs import (
    get_subvols, wait_for_volume_process_to_be_online)
from glustolibs.gluster.volume_ops import volume_start
from glustolibs.io.utils import wait_for_io_to_complete


@runs_on([[
    'arbiter', 'distributed-arbiter', 'replicated', 'distributed-replicated'
], ['glusterfs', 'nfs']])
class TestHealWithIO(GlusterBaseClass):
    def setUp(self):
        self.get_super_method(self, 'setUp')()

        # A single mount is enough for all the tests
        self.mounts = [self.mounts[0]]

        # For `test_heal_info_...` tests 6 replicas are needed
        if ('test_heal_info' in self.id().split('.')[-1]
                and self.volume_type.find('distributed') >= 0):
            self.volume['voltype']['dist_count'] = 6

        if not self.setup_volume_and_mount_volume(mounts=self.mounts):
            raise ExecutionError('Failed to setup and mount '
                                 '{}'.format(self.volname))

        self.client, self.m_point = (self.mounts[0].client_system,
                                     self.mounts[0].mountpoint)
        self.file_path = self.m_point + '/test_file'
        self._io_cmd = ('cat /dev/urandom | tr -dc [:space:][:print:] | '
                        'head -c {} ')
        # IO has to run for longer length for covering two scenarios in arbiter
        # volume type
        self.io_time = 600 if self.volume_type.find('arbiter') >= 0 else 300
        self.proc = ''

    def tearDown(self):
        if self.proc:
            ret = wait_for_io_to_complete([self.proc], [self.mounts[0]])
            if not ret:
                raise ExecutionError('Wait for IO completion failed on client')

        if not self.unmount_volume_and_cleanup_volume(mounts=self.mounts):
            raise ExecutionError('Not able to unmount and cleanup '
                                 '{}'.format(self.volname))
        self.get_super_method(self, 'tearDown')()

    def _validate_heal(self, timeout=8):
        """
        Validates `heal info` command returns in less than `timeout` value
        """
        start_time = time()
        ret, _, _ = heal_info(self.mnode, self.volname)
        end_time = time()
        self.assertEqual(ret, 0, 'Not able to query heal info status')
        self.assertLess(
            end_time - start_time, timeout,
            'Query of heal info of volume took more than {} '
            'seconds'.format(timeout))

    def _validate_io(self, delay=5):
        """
        Validates IO was happening during main test, measures by looking at
        time delay between issue and return of `async_communicate`
        """
        start_time = time()
        ret, _, err = self.proc.async_communicate()
        end_time = time()
        self.assertEqual(ret, 0, 'IO failed to complete with error '
                         '{}'.format(err))
        self.assertGreater(
            end_time - start_time, delay,
            'Unable to validate IO was happening during main test')
        self.proc = ''

    def _bring_brick_offline(self, bricks_list, arb_brick=False):
        """
        Bring arbiter brick offline if `arb_brick` is true else one of data
        bricks will be offline'd
        """
        # Pick up only `data` brick
        off_brick, b_type = bricks_list[:-1], 'data'
        if arb_brick:
            # Pick only `arbiter` brick
            off_brick, b_type = [bricks_list[-1]], 'arbiter'
        elif not arb_brick and self.volume_type.find('replicated') >= 0:
            # Should pick all bricks if voltype is `replicated`
            off_brick = bricks_list

        ret = bring_bricks_offline(self.volname, choice(off_brick))
        self.assertTrue(ret,
                        'Unable to bring `{}` brick offline'.format(b_type))

    def _get_hashed_subvol_index(self, subvols):
        """
        Return `index` of hashed_volume from list of subvols
        """
        index = 0
        if self.volume_type.find('distributed') >= 0:
            hashed_subvol, index = find_hashed_subvol(
                subvols, '',
                self.file_path.rsplit('/', 1)[1])
            self.assertIsNotNone(hashed_subvol,
                                 'Unable to find hashed subvolume')
        return index

    def _validate_brick_down_scenario(self,
                                      validate_heal=False,
                                      monitor_heal=False):
        """
        Refactor of common steps across volume type for validating brick down
        scenario
        """
        if validate_heal:
            # Wait for ample amount of IO to be written to file
            sleep(180)

            # Validate heal info shows o/p and exit in <8s
            self._validate_heal()

        # Force start volume and verify all process are online
        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, 'Unable to force start volume')

        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(
            ret, 'Not able to confirm all process of volume are online')

        if monitor_heal:
            # Wait for IO to be written to file
            sleep(30)

            # Monitor heal and validate data was appended successfully to file
            ret = monitor_heal_completion(self.mnode, self.volname)
            self.assertTrue(ret,
                            'Self heal is not completed post brick online')

    def _perform_heal_append_scenario(self):
        """
        Refactor of common steps in `entry_heal` and `data_heal` tests
        """
        # Find hashed subvol of the file with IO
        subvols = get_subvols(self.mnode, self.volname)['volume_subvols']
        index = self._get_hashed_subvol_index(subvols)

        # Bring down one of the `data` bricks of hashed subvol
        self._bring_brick_offline(bricks_list=subvols[index])

        cmd = ('{} >> {}; '.format(self._io_cmd.format('1G'), self.file_path))
        ret, _, _ = g.run(self.client, cmd)
        self.assertEqual(
            ret, 0, 'Unable to append 1G of data to existing '
            'file on mount post offline of a brick')

        # Start volume and verify all process are online
        self._validate_brick_down_scenario()

        # Start conitnuous IO and monitor heal completion
        cmd = ('count={}; while [ $count -gt 1 ]; do {} >> {}; sleep 1; '
               '((count--)); done;'.format(self.io_time,
                                           self._io_cmd.format('1M'),
                                           self.file_path))
        self.proc = g.run_async(self.client, cmd)
        self._validate_brick_down_scenario(monitor_heal=True)

        # Bring down `arbiter` brick and perform validation
        if self.volume_type.find('arbiter') >= 0:
            self._bring_brick_offline(bricks_list=subvols[index],
                                      arb_brick=True)
            self._validate_brick_down_scenario(monitor_heal=True)

        self._validate_io()

    def test_heal_info_with_io(self):
        """
        Description: Validate heal info command with IO

        Steps:
        - Create and mount a 6x3 replicated volume
        - Create a file and perform IO continuously on this file
        - While IOs are happening issue `heal info` command and validate o/p
          not taking much time
        """
        cmd = ('count=90; while [ $count -gt 1 ]; do {} >> {}; sleep 1; '
               '((count--)); done;'.format(self._io_cmd.format('5M'),
                                           self.file_path))
        self.proc = g.run_async(self.client, cmd)

        # Wait for IO to be written to file
        sleep(30)

        # Validate heal info shows o/p and exit in <5s
        self._validate_heal()

        # Validate IO was happening
        self._validate_io()

        g.log.info('Pass: Test heal info with IO is complete')

    def test_heal_info_with_io_and_brick_down(self):
        """
        Description: Validate heal info command with IO and brick down

        Steps:
        - Create and mount a 6x3 replicated volume
        - Create a file and perform IO continuously on this file
        - While IOs are happening, bring down one of the brick where the file
          is getting hashed to
        - After about a period of ~5 min issue `heal info` command and
          validate o/p not taking much time
        - Repeat the steps for arbiter on bringing arbiter brick down
        """
        cmd = ('count={}; while [ $count -gt 1 ]; do {} >> {}; sleep 1; '
               '((count--)); done;'.format(self.io_time,
                                           self._io_cmd.format('5M'),
                                           self.file_path))
        self.proc = g.run_async(self.client, cmd)

        # Wait for IO to be written to file
        sleep(30)

        # Find hashed subvol of the file with IO
        subvols = get_subvols(self.mnode, self.volname)['volume_subvols']
        index = self._get_hashed_subvol_index(subvols)

        # Bring down one of the `data` bricks of hashed subvol
        self._bring_brick_offline(bricks_list=subvols[index])

        # Validate heal and bring volume online
        self._validate_brick_down_scenario(validate_heal=True)

        # Bring down `arbiter` brick and perform validation
        if self.volume_type.find('arbiter') >= 0:
            self._bring_brick_offline(bricks_list=subvols[index],
                                      arb_brick=True)

            # Validate heal and bring volume online
            self._validate_brick_down_scenario(validate_heal=True)

        self._validate_io()

        g.log.info('Pass: Test heal info with IO and brick down is complete')

    def test_data_heal_on_file_append(self):
        """
        Description: Validate appends to a self healing file (data heal check)

        Steps:
        - Create and mount a 1x2 replicated volume
        - Create a file of ~ 1GB from the mount
        - Bring down a brick and write more data to the file
        - Bring up the offline brick and validate appending data to the file
          succeeds while file self heals
        - Repeat the steps for arbiter on bringing arbiter brick down
        """
        cmd = ('{} >> {}; '.format(self._io_cmd.format('1G'), self.file_path))
        ret, _, _ = g.run(self.client, cmd)
        self.assertEqual(ret, 0, 'Unable to create 1G of file on mount')

        # Perform `data_heal` test
        self._perform_heal_append_scenario()

        g.log.info('Pass: Test data heal on file append is complete')

    def test_entry_heal_on_file_append(self):
        """
        Description: Validate appends to a self healing file (entry heal check)

        Steps:
        - Create and mount a 1x2 replicated volume
        - Bring down a brick and write data to the file
        - Bring up the offline brick and validate appending data to the file
          succeeds while file self heals
        - Repeat the steps for arbiter on bringing arbiter brick down
        """

        # Perform `entry_heal` test
        self._perform_heal_append_scenario()

        g.log.info('Pass: Test entry heal on file append is complete')
