#  Copyright (C) 2017-2020 Red Hat, Inc. <http://www.redhat.com>
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

from glusto.core import Glusto as g

from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.heal_libs import (
    is_volume_in_split_brain, monitor_heal_completion,
    wait_for_self_heal_daemons_to_be_online)
from glustolibs.gluster.heal_ops import (enable_self_heal_daemon, trigger_heal,
                                         trigger_heal_full)
from glustolibs.gluster.lib_utils import collect_bricks_arequal, list_files
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.volume_ops import set_volume_options


# pylint: disable=stop-iteration-return, too-many-locals, too-many-statements
@runs_on([[
    'replicated', 'distributed-replicated', 'arbiter', 'distributed-arbiter'
], ['glusterfs']])
class TestSelfHeal(GlusterBaseClass):
    def setUp(self):
        self.get_super_method(self, 'setUp')()

        # A single mount is enough for the test
        self.mounts = self.mounts[0::-1]

        if not self.setup_volume_and_mount_volume(mounts=self.mounts):
            raise ExecutionError('Failed to setup and mount '
                                 '{}'.format(self.volname))

    def tearDown(self):
        if not self.unmount_volume_and_cleanup_volume(mounts=self.mounts):
            raise ExecutionError('Not able to unmount and cleanup '
                                 '{}'.format(self.volname))
        self.get_super_method(self, 'tearDown')()

    @staticmethod
    def _get_two_bricks(subvols, arbiter):
        """
        Yields two bricks from each subvol for dist/pure X arb/repl volumes
        """
        # Get an iterator for py2/3 compatibility
        brick_iter = iter(zip(*subvols))
        prev_brick = next(brick_iter)
        first_brick = prev_brick

        for index, curr_brick in enumerate(brick_iter, 1):
            # `yield` should contain arbiter brick for arbiter type vols
            if not (index == 1 and arbiter):
                yield prev_brick + curr_brick
            prev_brick = curr_brick
        # At the end yield first and last brick from a subvol
        yield prev_brick + first_brick

    def _get_files_in_brick(self, brick_path, dir_path):
        """
        Returns files in format of `dir_path/file_name` from the given brick
        path
        """
        node, path = brick_path.split(':')
        files = list_files(node, path, dir_path)
        self.assertIsNotNone(
            files, 'Unable to get list of files from {}'.format(brick_path))

        files = [file_name.rsplit('/', 1)[-1] for file_name in files]
        return [
            each_file for each_file in files
            if each_file in ('file1', 'file2', 'file3')
        ]

    def _run_cmd_and_assert(self, cmd):
        """
        Run `cmd` on `mnode` and assert for success
        """
        ret, _, err = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, '`{}` failed with {}'.format(cmd, err))

    def test_gfid_split_brain_resolution(self):
        """
        Description: Simulates gfid split brain on multiple files in a dir and
        resolve them via `bigger-file`, `mtime` and `source-brick` methods

        Steps:
        - Create and mount a replicated volume, create a dir and ~10 data files
        - Simulate gfid splits in 9 of the files
        - Resolve each 3 set of files using `bigger-file`, `mtime` and
          `source-bricks` split-brain resoultion methods
        - Trigger and monitor for heal completion
        - Validate all the files are healed and arequal matches for bricks in
          subvols
        """
        io_cmd = 'cat /dev/urandom | tr -dc [:space:][:print:] | head -c '
        client, m_point = (self.mounts[0].client_system,
                           self.mounts[0].mountpoint)
        arbiter = self.volume_type.find('arbiter') >= 0

        # Disable self-heal daemon and set `quorum-type` option to `none`
        ret = set_volume_options(self.mnode, self.volname, {
            'self-heal-daemon': 'off',
            'cluster.quorum-type': 'none'
        })
        self.assertTrue(
            ret, 'Not able to disable `quorum-type` and '
            '`self-heal` daemon volume options')

        # Create required dir and files from the mount
        split_dir = 'gfid_split_dir'
        file_io = ('cd %s; for i in {1..10}; do ' + io_cmd +
                   ' 1M > %s/file$i; done;')
        ret = mkdir(client, '{}/{}'.format(m_point, split_dir))
        self.assertTrue(ret, 'Unable to create a directory from mount point')
        ret, _, _ = g.run(client, file_io % (m_point, split_dir))

        # `file{4,5,6}` are re-created every time to be used in `bigger-file`
        # resolution method
        cmd = 'rm -rf {0}/file{1} && {2} {3}M > {0}/file{1}'
        split_cmds = {
            1:
            ';'.join(cmd.format(split_dir, i, io_cmd, 2) for i in range(1, 7)),
            2:
            ';'.join(cmd.format(split_dir, i, io_cmd, 3) for i in range(4, 7)),
            3: ';'.join(
                cmd.format(split_dir, i, io_cmd, 1) for i in range(4, 10)),
            4: ';'.join(
                cmd.format(split_dir, i, io_cmd, 1) for i in range(7, 10)),
        }

        # Get subvols and simulate entry split brain
        subvols = get_subvols(self.mnode, self.volname)['volume_subvols']
        self.assertTrue(subvols, 'Not able to get list of subvols')
        msg = ('Unable to bring files under {} dir to entry split brain while '
               '{} are down')
        for index, bricks in enumerate(self._get_two_bricks(subvols, arbiter),
                                       1):
            # Bring down two bricks from each subvol
            ret = bring_bricks_offline(self.volname, list(bricks))
            self.assertTrue(ret, 'Unable to bring {} offline'.format(bricks))

            ret, _, _ = g.run(client,
                              'cd {}; {}'.format(m_point, split_cmds[index]))
            self.assertEqual(ret, 0, msg.format(split_dir, bricks))

            # Bricks will be brought down only two times in case of arbiter and
            # bringing remaining files into split brain for `latest-mtime` heal
            if arbiter and index == 2:
                ret, _, _ = g.run(client,
                                  'cd {}; {}'.format(m_point, split_cmds[4]))
                self.assertEqual(ret, 0, msg.format(split_dir, bricks))

            # Bring offline bricks online
            ret = bring_bricks_online(
                self.mnode,
                self.volname,
                bricks,
                bring_bricks_online_methods='volume_start_force')
            self.assertTrue(ret, 'Unable to bring {} online'.format(bricks))

        # Enable self-heal daemon, trigger heal and assert volume is in split
        # brain condition
        ret = enable_self_heal_daemon(self.mnode, self.volname)
        self.assertTrue(ret, 'Failed to enable self heal daemon')

        ret = wait_for_self_heal_daemons_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, 'Not all self heal daemons are online')

        ret = trigger_heal(self.mnode, self.volname)
        self.assertTrue(ret, 'Unable to trigger index heal on the volume')

        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertTrue(ret, 'Volume should be in split brain condition')

        # Select source brick and take note of files in source brick
        stop = len(subvols[0]) - 1 if arbiter else len(subvols[0])
        source_bricks = [choice(subvol[0:stop]) for subvol in subvols]
        files = [
            self._get_files_in_brick(path, split_dir) for path in source_bricks
        ]

        # Resolve `file1, file2, file3` gfid split files using `source-brick`
        cmd = ('gluster volume heal ' + self.volname + ' split-brain '
               'source-brick {} /' + split_dir + '/{}')
        for index, source_brick in enumerate(source_bricks):
            for each_file in files[index]:
                run_cmd = cmd.format(source_brick, each_file)
                self._run_cmd_and_assert(run_cmd)

        # Resolve `file4, file5, file6` gfid split files using `bigger-file`
        cmd = ('gluster volume heal ' + self.volname +
               ' split-brain bigger-file /' + split_dir + '/{}')
        for each_file in ('file4', 'file5', 'file6'):
            run_cmd = cmd.format(each_file)
            self._run_cmd_and_assert(run_cmd)

        # Resolve `file7, file8, file9` gfid split files using `latest-mtime`
        cmd = ('gluster volume heal ' + self.volname +
               ' split-brain latest-mtime /' + split_dir + '/{}')
        for each_file in ('file7', 'file8', 'file9'):
            run_cmd = cmd.format(each_file)
            self._run_cmd_and_assert(run_cmd)

        # Unless `shd` is triggered manually/automatically files will still
        # appear in `heal info`
        ret = trigger_heal_full(self.mnode, self.volname)
        self.assertTrue(ret, 'Unable to trigger full self heal')

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(
            ret, 'All files in volume should be healed after healing files via'
            ' `source-brick`, `bigger-file`, `latest-mtime` methods manually')

        # Validate normal file `file10` and healed files don't differ in
        # subvols via an `arequal`
        for subvol in subvols:
            # Disregard last brick if volume is of arbiter type
            ret, arequal = collect_bricks_arequal(subvol[0:stop])
            self.assertTrue(
                ret, 'Unable to get `arequal` checksum on '
                '{}'.format(subvol[0:stop]))
            self.assertEqual(
                len(set(arequal)), 1, 'Mismatch of `arequal` '
                'checksum among {} is identified'.format(subvol[0:stop]))

        g.log.info('Pass: Resolution of gfid split-brain via `source-brick`, '
                   '`bigger-file` and `latest-mtime` methods is complete')
