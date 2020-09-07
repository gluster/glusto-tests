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

from glusto.core import Glusto as g

from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online, get_all_bricks)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.glusterfile import get_fattr
from glustolibs.gluster.heal_libs import is_volume_in_split_brain
from glustolibs.gluster.heal_ops import heal_info, heal_info_split_brain
from glustolibs.gluster.volume_ops import set_volume_options


# pylint: disable=too-many-locals, too-many-statements
@runs_on([['arbiter', 'replicated'], ['glusterfs']])
class TestSplitBrain(GlusterBaseClass):
    def setUp(self):
        self.get_super_method(self, 'setUp')()

        # A single mount is enough for all the tests
        self.mounts = [self.mounts[0]]

        if not self.setup_volume_and_mount_volume(mounts=self.mounts):
            raise ExecutionError('Failed to setup and mount '
                                 '{}'.format(self.volname))

    def tearDown(self):
        if not self.unmount_volume_and_cleanup_volume(mounts=self.mounts):
            raise ExecutionError('Not able to unmount and cleanup '
                                 '{}'.format(self.volname))
        self.get_super_method(self, 'tearDown')()

    def _run_cmd_and_validate(self, client, cmd, paths):
        """
        Run `cmd` from `paths` on `client`
        """
        for path in paths:
            ret, _, _ = g.run(client, cmd % path)
            self.assertEqual(
                ret, 0, 'Unable to perform `{}` from `{}` on `{}`'.format(
                    cmd, path, client))

    @staticmethod
    def _transform_gfids(gfids):
        """
        Returns  list of `gfids` joined by `-` at required places

        Example of one elemnt:
        Input:   0xd4653ea0289548eb81b35c91ffb73eff
        Returns: d4653ea0-2895-48eb-81b3-5c91ffb73eff
        """
        split_pos = [10, 14, 18, 22]
        rout = []
        for gfid in gfids:
            rout.append('-'.join(
                gfid[start:stop]
                for start, stop in zip([2] + split_pos, split_pos + [None])))
        return rout

    def test_split_brain_from_heal_command(self):
        """
        Description: Simulate and validate data, metadata and entry split brain

        Steps:
        - Create and mount a replicated volume and disable quorum, self-heal
          deamon
        - Create ~10 files from the mount point and simulate data, metadata
          split-brain for 2 files each
        - Create a dir with some files and simulate entry/gfid split brain
        - Validate volume successfully recognizing split-brain
        - Validate a lookup on split-brain files fails with EIO error on mount
        - Validate `heal info` and `heal info split-brain` command shows only
          the files that are in split-brain
        - Validate new files and dir's can be created from the mount
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

        # Create required dir's from the mount
        fqpath = '{}/dir'.format(m_point)
        file_io = ('cd %s; for i in {1..6}; do ' + io_cmd +
                   ' 2M > file$i; done;')
        file_cmd = 'cd %s; touch file{7..10}'
        ret = mkdir(client, fqpath)
        self.assertTrue(ret, 'Unable to create a directory from mount point')

        # Create empty files and data files
        for cmd in (file_io, file_cmd):
            self._run_cmd_and_validate(client, cmd, [m_point, fqpath])

        all_bricks = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(
            all_bricks, 'Unable to get list of bricks '
            'associated with the volume')

        # Data will be appended to the files `file1, file2` resulting in data
        # split brain
        data_split_cmd = ';'.join(io_cmd + '2M >> ' + each_file
                                  for each_file in ('file1', 'file2'))

        # File permissions will be changed for `file4, file5` to result in
        # metadata split brain
        meta_split_cmd = ';'.join('chmod 0555 ' + each_file
                                  for each_file in ('file4', 'file5'))

        # Files will be deleted and created with data to result in data,
        # metadata split brain on files and entry(gfid) split brain on dir
        entry_split_cmd = ';'.join('rm -f ' + each_file + ' && ' + io_cmd +
                                   ' 2M > ' + each_file
                                   for each_file in ('dir/file1', 'dir/file2'))

        # Need to always select arbiter(3rd) brick if volume is arbiter type or
        # any two bricks for replicated volume
        for bricks in zip(all_bricks, all_bricks[1:] + [all_bricks[0]]):

            # Skip iteration if volume type is arbiter and `bricks` doesn't
            # contain arbiter brick
            if arbiter and (all_bricks[-1] not in bricks):
                continue

            # Bring bricks offline
            ret = bring_bricks_offline(self.volname, list(bricks))
            self.assertTrue(ret, 'Unable to bring {} offline'.format(bricks))

            # Run cmd to bring files into split brain
            for cmd, msg in ((data_split_cmd, 'data'),
                             (meta_split_cmd, 'meta'), (entry_split_cmd,
                                                        'entry')):
                ret, _, _ = g.run(client, 'cd {}; {}'.format(m_point, cmd))
                self.assertEqual(
                    ret, 0, 'Unable to run cmd for bringing files '
                    'into {} split brain'.format(msg))

            # Bring offline bricks online
            ret = bring_bricks_online(
                self.mnode,
                self.volname,
                bricks,
                bring_bricks_online_methods='volume_start_force')
            self.assertTrue(ret, 'Unable to bring {} online'.format(bricks))

        # Validate volume is in split-brain
        self.assertTrue(is_volume_in_split_brain(self.mnode, self.volname),
                        'Volume should be in split-brain')

        # Validate `head` lookup on split brain files fails with EIO
        for each_file in ('file1', 'file2', 'file4', 'file5', 'dir/file1',
                          'dir/file2'):
            ret, _, err = g.run(client,
                                'cd {}; head {}'.format(m_point, each_file))
            self.assertNotEqual(
                ret, 0, 'Lookup on split-brain file {} should '
                'fail'.format(each_file))
            self.assertIn(
                'Input/output error', err,
                'File {} should result in EIO error'.format(each_file))

        # Validate presence of split-brain files and absence of other files in
        # `heal info` and `heal info split-brain` commands
        ret, info, _ = heal_info(self.mnode, self.volname)
        self.assertEqual(ret, 0, 'Unable to query for `heal info`')
        ret, info_spb, _ = heal_info_split_brain(self.mnode, self.volname)
        self.assertEqual(ret, 0, 'Unable to query for `heal info split-brain`')

        # Collect `gfid's` of files in data and metadata split-brain
        common_gfids = []
        host, path = all_bricks[0].split(':')
        for each_file in ('file1', 'file2', 'file4', 'file5', 'dir'):
            fattr = get_fattr(host, path + '/{}'.format(each_file),
                              'trusted.gfid')
            self.assertIsNotNone(
                fattr, 'Unable to get `gfid` for {}'.format(each_file))
            common_gfids.append(fattr)

        # GFID for files under an entry split brain dir differs from it's peers
        uniq_gfids = []
        for brick in all_bricks[:-1] if arbiter else all_bricks:
            host, path = brick.split(':')
            for each_file in ('dir/file1', 'dir/file2'):
                fattr = get_fattr(host, path + '/{}'.format(each_file),
                                  'trusted.gfid')
                self.assertIsNotNone(
                    fattr, 'Unable to get `gfid` for {}'.format(each_file))
                uniq_gfids.append(fattr)

        # Transform GFIDs to match against o/p of `heal info` and `split-brain`
        common_gfids[:] = self._transform_gfids(common_gfids)
        uniq_gfids[:] = self._transform_gfids(uniq_gfids)

        # Just enough validation by counting occurences asserting success
        common_files = ['/file1 -', '/file2 -', '/file4', '/file5', '/dir ']
        uniq_files = ['/dir/file1', '/dir/file2']

        # Common files should occur 3 times each in `heal info` and
        # `heal info split-brain` or 2 times for arbiter
        occur = 2 if arbiter else 3
        for each_file, gfid in zip(common_files, common_gfids):

            # Check against `heal info` cmd
            self.assertEqual(
                info.count(gfid) + info.count(each_file), occur,
                'File {} with gfid {} should exist in `heal info` '
                'command'.format(each_file[:6], gfid))

            # Check against `heal info split-brain` cmd
            self.assertEqual(
                info_spb.count(gfid) + info_spb.count(each_file[:6].rstrip()),
                occur, 'File {} with gfid {} should exist in `heal info '
                'split-brain` command'.format(each_file[:6], gfid))

        # Entry split files will be listed only in `heal info` cmd
        for index, each_file in enumerate(uniq_files):

            # Collect file and it's associated gfid's
            entries = (uniq_files + uniq_gfids)[index::2]
            count = sum(info.count(entry) for entry in entries)
            self.assertEqual(
                count, occur, 'Not able to find existence of '
                'entry split brain file {} in `heal info`'.format(each_file))

        # Assert no other file is counted as in split-brain
        for cmd, rout, exp_str in (('heal info', info, 'entries: 7'),
                                   ('heal info split-brain', info_spb,
                                    'split-brain: 5')):
            self.assertEqual(
                rout.count(exp_str), occur, 'Each node should '
                'list only {} entries in {} command'.format(exp_str[-1], cmd))

        # Validate new files and dir can be created from mount
        fqpath = '{}/temp'.format(m_point)
        ret = mkdir(client, fqpath)
        self.assertTrue(
            ret, 'Unable to create a dir from mount post split-brain of files')
        for cmd in (file_io, file_cmd):
            self._run_cmd_and_validate(client, cmd, [fqpath])

        g.log.info('Pass: Validated data, metadata and entry split brain')
