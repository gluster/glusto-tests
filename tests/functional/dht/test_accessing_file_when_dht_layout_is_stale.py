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

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.glusterfile import get_fattr, set_fattr
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.io.utils import collect_mounts_arequal


# pylint: disable=too-many-locals
@runs_on([['distributed'], ['glusterfs']])
class TestAccessFileStaleLayout(GlusterBaseClass):
    def setUp(self):
        self.get_super_method(self, 'setUp')()

        self.volume['voltype']['dist_count'] = 2
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError('Failed to setup and mount volume')

    def tearDown(self):
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError('Failed to umount and cleanup Volume')

        self.get_super_method(self, 'tearDown')()

    def _get_brick_node_and_path(self):
        '''Yields list containing brick node and path from first brick of each
           subvol
        '''
        subvols = get_subvols(self.mnode, self.volname)
        for subvol in subvols['volume_subvols']:
            subvol[0] += self.dir_path
            yield subvol[0].split(':')

    def _assert_file_lookup(self, node, fqpath, when, result):
        '''Perform `stat` on `fqpath` from `node` and validate against `result`
        '''
        cmd = ('stat {}'.format(fqpath))
        ret, _, _ = g.run(node, cmd)
        assert_method = self.assertNotEqual
        assert_msg = 'fail'
        if result:
            assert_method = self.assertEqual
            assert_msg = 'pass'
        assert_method(
            ret, 0, 'Lookup on {} from {} should {} {} layout '
            'change'.format(fqpath, node, assert_msg, when))

    def test_accessing_file_when_dht_layout_is_stale(self):
        '''
        Description : Checks if a file can be opened and accessed if the dht
                      layout has become stale.

        Steps:
        1. Create, start and mount a volume consisting 2 subvols on 2 clients
        2. Create a dir `dir` and file `dir/file` from client0
        3. Take note of layouts of `brick1`/dir and `brick2`/dir of the volume
        4. Validate for success lookup from only one brick path
        5. Re-assign layouts ie., brick1/dir to brick2/dir and vice-versa
        6. Remove `dir/file` from client0 and recreate same file from client0
           and client1
        7. Validate for success lookup from only one brick path (as layout is
           changed file creation path will be changed)
        8. Validate checksum is matched from both the clients
        '''

        # Will be used in _get_brick_node_and_path
        self.dir_path = '/dir'

        # Will be used in argument to _assert_file_lookup
        file_name = '/file'

        dir_path = self.mounts[0].mountpoint + self.dir_path
        file_path = dir_path + file_name

        client0, client1 = self.clients[0], self.clients[1]
        fattr = 'trusted.glusterfs.dht'
        io_cmd = ('cat /dev/urandom | tr -dc [:space:][:print:] | '
                  'head -c 1K > {}'.format(file_path))

        # Create a dir from client0
        ret = mkdir(self.clients[0], dir_path)
        self.assertTrue(ret, 'Unable to create a directory from mount point')

        # Touch a file with data from client0
        ret, _, _ = g.run(client0, io_cmd)
        self.assertEqual(ret, 0, 'Failed to create a file on mount')

        # Yields `node` and `brick-path` from first brick of each subvol
        gen = self._get_brick_node_and_path()

        # Take note of newly created directory's layout from org_subvol1
        node1, fqpath1 = next(gen)
        layout1 = get_fattr(node1, fqpath1, fattr)
        self.assertIsNotNone(layout1,
                             '{} is not present on {}'.format(fattr, fqpath1))

        # Lookup on file from node1 should fail as `dir/file` will always get
        # hashed to node2 in a 2-brick distribute volume by default
        self._assert_file_lookup(node1,
                                 fqpath1 + file_name,
                                 when='before',
                                 result=False)

        # Take note of newly created directory's layout from org_subvol2
        node2, fqpath2 = next(gen)
        layout2 = get_fattr(node2, fqpath2, fattr)
        self.assertIsNotNone(layout2,
                             '{} is not present on {}'.format(fattr, fqpath2))

        # Lookup on file from node2 should pass
        self._assert_file_lookup(node2,
                                 fqpath2 + file_name,
                                 when='before',
                                 result=True)

        # Set org_subvol2 directory layout to org_subvol1 and vice-versa
        for node, fqpath, layout, vol in ((node1, fqpath1, layout2, (2, 1)),
                                          (node2, fqpath2, layout1, (1, 2))):
            ret = set_fattr(node, fqpath, fattr, layout)
            self.assertTrue(
                ret, 'Failed to set layout of org_subvol{} on '
                'brick {} of org_subvol{}'.format(vol[0], fqpath, vol[1]))

        # Remove file after layout change from client0
        cmd = 'rm -f {}'.format(file_path)
        ret, _, _ = g.run(client0, cmd)
        self.assertEqual(ret, 0, 'Failed to delete file after layout change')

        # Create file with same name as above after layout change from client0
        # and client1
        for client in (client0, client1):
            ret, _, _ = g.run(client, io_cmd)
            self.assertEqual(
                ret, 0, 'Failed to create file from '
                '{} after layout change'.format(client))

        # After layout change lookup on file from node1 should pass
        self._assert_file_lookup(node1,
                                 fqpath1 + file_name,
                                 when='after',
                                 result=True)

        # After layout change lookup on file from node2 should fail
        self._assert_file_lookup(node2,
                                 fqpath2 + file_name,
                                 when='after',
                                 result=False)

        # Take note of checksum from client0 and client1
        checksums = [None] * 2
        for index, mount in enumerate(self.mounts):
            ret, checksums[index] = collect_mounts_arequal(mount, dir_path)
            self.assertTrue(
                ret, 'Failed to get arequal on client {}'.format(
                    mount.client_system))

        # Validate no checksum mismatch
        self.assertEqual(checksums[0], checksums[1],
                         'Checksum mismatch between client0 and client1')

        g.log.info('Pass: Test accessing file on stale layout is complete.')
