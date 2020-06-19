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
from time import sleep

from glusto.core import Glusto as g

from glustolibs.gluster.brick_libs import bring_bricks_offline
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.glusterfile import create_link_file
from glustolibs.gluster.heal_libs import monitor_heal_completion
from glustolibs.gluster.heal_ops import heal_info
from glustolibs.gluster.volume_libs import get_subvols, volume_start
from glustolibs.io.utils import (collect_mounts_arequal, validate_io_procs,
                                 wait_for_io_to_complete)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['dispersed', 'distributed-dispersed'], ['glusterfs', 'nfs']])
class TestIOsOnECVolume(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()
        cls.script_path = '/usr/share/glustolibs/io/scripts'
        for file_ops in ('file_dir_ops.py', 'fd_writes.py'):
            ret = upload_scripts(cls.clients,
                                 '{}/{}'.format(cls.script_path, file_ops))
            if not ret:
                raise ExecutionError('Failed to upload IO scripts to client')

    def setUp(self):
        self.get_super_method(self, 'setUp')()
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        self.all_mounts_procs = []
        if not ret:
            raise ExecutionError('Failed to setup and mount volume')

    def tearDown(self):
        if self.all_mounts_procs:
            ret = wait_for_io_to_complete(self.all_mounts_procs,
                                          [self.mounts[1]] *
                                          len(self.all_mounts_procs))
            if not ret:
                raise ExecutionError('Wait for IO completion failed on some '
                                     'of the clients')
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Not able to unmount and cleanup volume")
        self.get_super_method(self, 'tearDown')()

    def _bring_bricks_online_and_monitor_heal(self, bricks):
        """Bring the bricks online and monitor heal until completion"""
        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, 'Not able to force start volume')
        ret = monitor_heal_completion(self.mnode,
                                      self.volname,
                                      bricks=list(bricks))
        self.assertTrue(ret, 'Heal is not complete for {}'.format(bricks))

    # pylint: disable=too-many-locals
    def test_io_with_cyclic_brick_down(self):
        """
        Description: To check heal process on EC volume when brick is brought
                    down in a cyclic fashion
        Steps:
        - Create, start and mount an EC volume in two clients
        - Create multiple files and directories including all file types on one
          directory from client 1
        - Take arequal check sum of above data
        - Create another folder and pump different fops from client 2
        - Fail and bring up redundant bricks in a cyclic fashion in all of the
          subvols maintaining a minimum delay between each operation
        - In every cycle create new dir when brick is down and wait for heal
        - Validate heal info on volume when brick down erroring out instantly
        - Validate arequal on brining the brick offline
        """

        # Create a directory structure on mount from client 1
        mount_obj = self.mounts[0]
        cmd = ('/usr/bin/env python {}/file_dir_ops.py '
               'create_deep_dirs_with_files --dir-depth 3 '
               '--max-num-of-dirs 5 --fixed-file-size 10k '
               '--num-of-files 9 {}'.format(
                   self.script_path,
                   mount_obj.mountpoint,
               ))
        ret, _, _ = g.run(mount_obj.client_system, cmd)
        self.assertEqual(ret, 0, 'Not able to create directory structure')
        dir_name = 'user1'
        for i in range(5):
            ret = create_link_file(
                mount_obj.client_system,
                '{}/{}/testfile{}.txt'.format(mount_obj.mountpoint, dir_name,
                                              i),
                '{}/{}/testfile{}_sl.txt'.format(mount_obj.mountpoint,
                                                 dir_name, i),
                soft=True)
        self.assertTrue(ret, 'Not able to create soft links')
        for i in range(5, 9):
            ret = create_link_file(
                mount_obj.client_system,
                '{}/{}/testfile{}.txt'.format(mount_obj.mountpoint, dir_name,
                                              i),
                '{}/{}/testfile{}_hl.txt'.format(mount_obj.mountpoint,
                                                 dir_name, i))
        self.assertTrue(ret, 'Not able to create hard links')
        g.log.info('Successfully created directory structure consisting all '
                   'file types on mount')

        # Take note of arequal checksum
        ret, exp_arequal = collect_mounts_arequal(mount_obj, path=dir_name)
        self.assertTrue(ret, 'Failed to get arequal checksum on mount')

        # Get all the subvols in the volume
        subvols = get_subvols(self.mnode, self.volname)
        self.assertTrue(subvols.get('volume_subvols'), 'Not able to get '
                        'subvols of the volume')

        # Create a dir, pump IO in that dir, offline b1, wait for IO and
        # online b1, wait for heal of b1, bring b2 offline...
        m_point, m_client = (self.mounts[1].mountpoint,
                             self.mounts[1].client_system)
        cur_off_bricks = ''
        for count, off_brick in enumerate(zip(*subvols.get('volume_subvols')),
                                          start=1):

            # Bring offline bricks online by force starting volume
            if cur_off_bricks:
                self._bring_bricks_online_and_monitor_heal(cur_off_bricks)

            # Create a dir for running IO
            ret = mkdir(m_client, '{}/dir{}'.format(m_point, count))
            self.assertTrue(
                ret, 'Not able to create directory for '
                'starting IO before offline of brick')

            # Start IO in the newly created directory
            cmd = ('/usr/bin/env python {}/fd_writes.py -n 10 -t 480 -d 5 -c '
                   '16 --dir {}/dir{}'.format(self.script_path, m_point,
                                              count))
            proc = g.run_async(m_client, cmd)
            self.all_mounts_procs.append(proc)

            # Wait IO to partially fill the dir
            sleep(10)

            # Bring a single brick offline from all of subvols
            ret = bring_bricks_offline(self.volname, list(off_brick))
            self.assertTrue(ret,
                            'Not able to bring {} offline'.format(off_brick))

            # Validate heal info errors out, on brining bricks offline in < 5s
            start_time = datetime.now().replace(microsecond=0)
            ret, _, _ = heal_info(self.mnode, self.volname)
            end_time = datetime.now().replace(microsecond=0)
            self.assertEqual(
                ret, 0, 'Not able to query heal info status '
                'of volume when a brick is offline')
            self.assertLess(
                end_time - start_time, timedelta(seconds=5),
                'Query of heal info of volume when a brick is '
                'offline is taking more than 5 seconds')

            # Wait for some more IO to fill dir
            sleep(10)

            # Validate arequal on initial static dir
            ret, act_arequal = collect_mounts_arequal(mount_obj, path=dir_name)
            self.assertTrue(
                ret, 'Failed to get arequal checksum on bringing '
                'a brick offline')
            self.assertEqual(
                exp_arequal, act_arequal, 'Mismatch of arequal '
                'checksum before and after killing a brick')

            cur_off_bricks = off_brick

        # Take note of ctime on mount
        ret, prev_ctime, _ = g.run(m_client, 'date +%s')
        self.assertEqual(ret, 0, 'Not able to get epoch time from client')

        self._bring_bricks_online_and_monitor_heal(cur_off_bricks)

        # Validate IO was happening during brick operations
        # and compare ctime of recent file to current epoch time
        ret = validate_io_procs(self.all_mounts_procs,
                                [self.mounts[0]] * len(self.all_mounts_procs))
        self.assertTrue(ret, 'Not able to validate completion of IO on mounts')
        self.all_mounts_procs *= 0  # don't validate IO in tearDown
        ret, curr_ctime, _ = g.run(
            m_client, "find {} -printf '%C@\n' -type f | "
            'sort -r | head -n 1'.format(m_point))
        self.assertEqual(
            ret, 0, 'Not able to get ctime of last edited file from the mount')
        self.assertGreater(
            float(curr_ctime), float(prev_ctime), 'Not able '
            'to validate IO was happening during brick operations')

        g.log.info('Completed IO continuity test on EC volume successfully')
