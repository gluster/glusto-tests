#  Copyright (C) 2021 Red Hat, Inc. <http://www.redhat.com>
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

from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online,
                                           get_online_bricks_list)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.heal_libs import (
    is_heal_complete, is_volume_in_split_brain, monitor_heal_completion,
    wait_for_self_heal_daemons_to_be_online)
from glustolibs.gluster.heal_ops import (disable_self_heal_daemon,
                                         enable_self_heal_daemon, trigger_heal)
from glustolibs.gluster.lib_utils import (add_user, collect_bricks_arequal,
                                          del_user, group_add, group_del)
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.io.utils import list_all_files_and_dirs_mounts


@runs_on([['arbiter', 'replicated'], ['glusterfs']])
class TestMetadataAndDataHeal(GlusterBaseClass):
    '''Description: Verify shd heals files after performing metadata and data
    operations while a brick was down'''
    def _dac_helper(self, host, option):
        '''Helper for creating, deleting users and groups'''

        # Permission/Ownership changes required only for `test_metadata..`
        # tests, using random group and usernames
        if 'metadata' not in self.test_dir:
            return

        if option == 'create':
            # Groups
            for group in ('qa_func', 'qa_system'):
                if not group_add(host, group):
                    raise ExecutionError('Unable to {} group {} on '
                                         '{}'.format(option, group, host))

            # User
            if not add_user(host, 'qa_all', group='qa_func'):
                raise ExecutionError('Unable to {} user {} under {} on '
                                     '{}'.format(option, 'qa_all', 'qa_func',
                                                 host))
        elif option == 'delete':
            # Groups
            for group in ('qa_func', 'qa_system'):
                if not group_del(host, group):
                    raise ExecutionError('Unable to {} group {} on '
                                         '{}'.format(option, group, host))

            # User
            if not del_user(host, 'qa_all'):
                raise ExecutionError('Unable to {} user on {}'.format(
                    option, host))

    def setUp(self):
        self.get_super_method(self, 'setUp')()

        # A single mount is enough for all the tests
        self.mounts = self.mounts[0:1]
        self.client = self.mounts[0].client_system

        # Use testcase name as test directory
        self.test_dir = self.id().split('.')[-1]
        self.fqpath = self.mounts[0].mountpoint + '/' + self.test_dir

        if not self.setup_volume_and_mount_volume(mounts=self.mounts):
            raise ExecutionError('Failed to setup and mount '
                                 '{}'.format(self.volname))

        # Crete group and user names required for the test
        self._dac_helper(host=self.client, option='create')

    def tearDown(self):
        # Delete group and user names created as part of setup
        self._dac_helper(host=self.client, option='delete')

        if not self.unmount_volume_and_cleanup_volume(mounts=self.mounts):
            raise ExecutionError('Not able to unmount and cleanup '
                                 '{}'.format(self.volname))

        self.get_super_method(self, 'tearDown')()

    def _perform_io_and_disable_self_heal(self):
        '''Refactor of steps common to all tests: Perform IO, disable heal'''
        ret = mkdir(self.client, self.fqpath)
        self.assertTrue(ret,
                        'Directory creation failed on {}'.format(self.client))
        self.io_cmd = 'cat /dev/urandom | tr -dc [:space:][:print:] | head -c '
        # Create 6 dir's, 6 files and 6 files in each subdir with 10K data
        file_io = ('''cd {0}; for i in `seq 1 6`;
                    do mkdir dir.$i; {1} 10K > file.$i;
                    for j in `seq 1 6`;
                    do {1} 10K > dir.$i/file.$j; done;
                    done;'''.format(self.fqpath, self.io_cmd))
        ret, _, err = g.run(self.client, file_io)
        self.assertEqual(ret, 0, 'Unable to create directories and data files')
        self.assertFalse(err, '{0} failed with {1}'.format(file_io, err))

        # Disable self heal deamon
        self.assertTrue(disable_self_heal_daemon(self.mnode, self.volname),
                        'Disabling self-heal-daemon falied')

    def _perform_brick_ops_and_enable_self_heal(self, op_type):
        '''Refactor of steps common to all tests: Brick down and perform
        metadata/data operations'''
        # First brick in the subvol will always be online and used for self
        # heal, so make keys match brick index
        self.op_cmd = {
            # Metadata Operations (owner and permission changes)
            'metadata': {
                2:
                '''cd {0}; for i in `seq 1 3`; do chown -R qa_all:qa_func \
                dir.$i file.$i; chmod -R 555 dir.$i file.$i; done;''',
                3:
                '''cd {0}; for i in `seq 1 3`; do chown -R :qa_system \
                dir.$i file.$i; chmod -R 777 dir.$i file.$i; done;''',
                # 4 - Will be used for final data consistency check
                4:
                '''cd {0}; for i in `seq 1 6`; do chown -R qa_all:qa_system \
                dir.$i file.$i; chmod -R 777 dir.$i file.$i; done;''',
            },
            # Data Operations (append data to the files)
            'data': {
                2:
                '''cd {0}; for i in `seq 1 3`;
                    do {1} 2K >> file.$i;
                    for j in `seq 1 3`;
                    do {1} 2K >> dir.$i/file.$j; done;
                    done;''',
                3:
                '''cd {0}; for i in `seq 1 3`;
                    do {1} 3K >> file.$i;
                    for j in `seq 1 3`;
                    do {1} 3K >> dir.$i/file.$j; done;
                    done;''',
                # 4 - Will be used for final data consistency check
                4:
                '''cd {0}; for i in `seq 1 6`;
                    do {1} 4K >> file.$i;
                    for j in `seq 1 6`;
                    do {1} 4K >> dir.$i/file.$j; done;
                    done;''',
            },
        }
        bricks = get_online_bricks_list(self.mnode, self.volname)
        self.assertIsNotNone(bricks,
                             'Not able to get list of bricks in the volume')

        # Make first brick always online and start operations from second brick
        for index, brick in enumerate(bricks[1:], start=2):

            # Bring brick offline
            ret = bring_bricks_offline(self.volname, brick)
            self.assertTrue(ret, 'Unable to bring {} offline'.format(bricks))

            # Perform metadata/data operation
            cmd = self.op_cmd[op_type][index].format(self.fqpath, self.io_cmd)
            ret, _, err = g.run(self.client, cmd)
            self.assertEqual(ret, 0, '{0} failed with {1}'.format(cmd, err))
            self.assertFalse(err, '{0} failed with {1}'.format(cmd, err))

            # Bring brick online
            ret = bring_bricks_online(
                self.mnode,
                self.volname,
                brick,
                bring_bricks_online_methods='volume_start_force')

        # Assert metadata/data operations resulted in pending heals
        self.assertFalse(is_heal_complete(self.mnode, self.volname))

        # Enable and wait self heal daemon to be online
        self.assertTrue(enable_self_heal_daemon(self.mnode, self.volname),
                        'Enabling self heal daemon failed')
        self.assertTrue(
            wait_for_self_heal_daemons_to_be_online(self.mnode, self.volname),
            'Not all self heal daemons are online')

    def _validate_heal_completion_and_arequal(self, op_type):
        '''Refactor of steps common to all tests: Validate heal from heal
        commands, verify arequal, perform IO and verify arequal after IO'''

        # Validate heal completion
        self.assertTrue(monitor_heal_completion(self.mnode, self.volname),
                        'Self heal is not completed within timeout')
        self.assertFalse(
            is_volume_in_split_brain(self.mnode, self.volname),
            'Volume is in split brain even after heal completion')

        subvols = get_subvols(self.mnode, self.volname)['volume_subvols']
        self.assertTrue(subvols, 'Not able to get list of subvols')
        arbiter = self.volume_type.find('arbiter') >= 0
        stop = len(subvols[0]) - 1 if arbiter else len(subvols[0])

        # Validate arequal
        self._validate_arequal_and_perform_lookup(subvols, stop)

        # Perform some additional metadata/data operations
        cmd = self.op_cmd[op_type][4].format(self.fqpath, self.io_cmd)
        ret, _, err = g.run(self.client, cmd)
        self.assertEqual(ret, 0, '{0} failed with {1}'.format(cmd, err))
        self.assertFalse(err, '{0} failed with {1}'.format(cmd, err))

        # Validate arequal after additional operations
        self._validate_arequal_and_perform_lookup(subvols, stop)

    def _validate_arequal_and_perform_lookup(self, subvols, stop):
        '''Refactor of steps common to all tests: Validate arequal from bricks
        backend and perform a lookup of all files from mount'''
        for subvol in subvols:
            ret, arequal = collect_bricks_arequal(subvol[0:stop])
            self.assertTrue(
                ret, 'Unable to get `arequal` checksum on '
                '{}'.format(subvol[0:stop]))
            self.assertEqual(
                len(set(arequal)), 1, 'Mismatch of `arequal` '
                'checksum among {} is identified'.format(subvol[0:stop]))

        # Perform a lookup of all files and directories on mounts
        self.assertTrue(list_all_files_and_dirs_mounts(self.mounts),
                        'Failed to list all files and dirs from mount')

    def test_metadata_heal_from_shd(self):
        '''Description: Verify files heal after switching on `self-heal-daemon`
        when metadata operations are performed while a brick was down

        Steps:
        1. Create, mount and run IO on volume
        2. Set `self-heal-daemon` to `off`, cyclic brick down and perform
           metadata operations
        3. Set `self-heal-daemon` to `on` and wait for heal completion
        4. Validate areequal checksum on backend bricks
        '''
        op_type = 'metadata'
        self._perform_io_and_disable_self_heal()
        self._perform_brick_ops_and_enable_self_heal(op_type=op_type)
        self._validate_heal_completion_and_arequal(op_type=op_type)
        g.log.info('Pass: Verification of metadata heal after switching on '
                   '`self heal daemon` is complete')

    def test_metadata_heal_from_heal_cmd(self):
        '''Description: Verify files heal after triggering heal command when
        metadata operations are performed while a brick was down

        Steps:
        1. Create, mount and run IO on volume
        2. Set `self-heal-daemon` to `off`, cyclic brick down and perform
        metadata operations
        3. Set `self-heal-daemon` to `on`, invoke `gluster vol <vol> heal`
        4. Validate areequal checksum on backend bricks
        '''
        op_type = 'metadata'
        self._perform_io_and_disable_self_heal()
        self._perform_brick_ops_and_enable_self_heal(op_type=op_type)

        # Invoke `glfsheal`
        self.assertTrue(trigger_heal(self.mnode, self.volname),
                        'Unable to trigger index heal on the volume')

        self._validate_heal_completion_and_arequal(op_type=op_type)
        g.log.info(
            'Pass: Verification of metadata heal via `glfsheal` is complete')

    def test_data_heal_from_shd(self):
        '''Description: Verify files heal after triggering heal command when
        data operations are performed while a brick was down

        Steps:
        1. Create, mount and run IO on volume
        2. Set `self-heal-daemon` to `off`, cyclic brick down and perform data
           operations
        3. Set `self-heal-daemon` to `on` and wait for heal completion
        4. Validate areequal checksum on backend bricks
        '''
        op_type = 'data'
        self._perform_io_and_disable_self_heal()
        self._perform_brick_ops_and_enable_self_heal(op_type=op_type)
        self._validate_heal_completion_and_arequal(op_type=op_type)
        g.log.info('Pass: Verification of data heal after switching on '
                   '`self heal daemon` is complete')
