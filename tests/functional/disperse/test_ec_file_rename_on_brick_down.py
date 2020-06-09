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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-131 USA.

from random import choice
from time import sleep

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import get_all_bricks, bring_bricks_offline
from glustolibs.gluster.volume_libs import volume_start
from glustolibs.gluster.glusterfile import create_link_file


@runs_on([['replicated', 'dispersed', 'distributed-dispersed'], ['glusterfs']])
class TestECRenameFilesOnBrickDown(GlusterBaseClass):

    # pylint: disable=too-many-statements,too-many-locals
    def setUp(self):
        self.get_super_method(self, 'setUp')()

        # Remove on fixing BZ 1596165
        if 'dispersed' in self.volname:
            self.skipTest("Test will fail due to BZ 1596165")

        # Setup and mount volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup and mount volume")

    def tearDown(self):

        # Unmount and cleanup volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to unmount and cleanup volume")

        self.get_super_method(self, 'tearDown')()

    def create_links(self, client, path):

        # Soft links
        for i in range(4, 7):
            ret = create_link_file(client,
                                   '{}/file{}_or'.format(path, i),
                                   '{}/file{}_sl'.format(path, i), soft=True)
            self.assertTrue(ret, "Fail: Not able to create soft link for "
                            "{}/file{}_or".format(path, i))
        g.log.info("Created soft links for files successfully")

        # Hard links
        for i in range(7, 10):
            ret = create_link_file(client,
                                   '{}/file{}_or'.format(path, i),
                                   '{}/file{}_hl'.format(path, i),)
            self.assertTrue(ret, "Fail: Not able to create hard link for "
                            "{}/file{}_or".format(path, i))
        g.log.info("Created hard links for files successfully")

    def test_ec_rename_files_with_brick_down(self):
        """
        Description: Test to check no errors on file/dir renames when one of
                        the bricks is down in the volume.
        Steps:
        1. Create an EC volume
        2. Mount the volume using FUSE on two different clients
        3. Create ~9 files from one of the client
        4. Create ~9 dir with ~9 files each from another client
        5. Create soft-links, hard-links for file{4..6}, file{7..9}
        6. Create soft-links for dir{4..6}
        7. Begin renaming the files, in multiple iterations
        8. Bring down a brick while renaming the files
        9. Bring the brick online after renaming some of the files
        10. Wait for renaming of the files
        11. Validate no data loss and files are renamed successfully
        """

        # Creating ~9 files from client 1 on mount
        m_point = self.mounts[0].mountpoint
        cmd = 'cd %s; touch file{1..9}_or' % m_point
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Fail: Not able to create files on "
                         "{}".format(m_point))
        g.log.info("Files created successfully on mount point")

        # Creating 9 dir X 9 files in each dir from client 2
        cmd = ('cd %s; mkdir -p dir{1..9}_or; touch '
               'dir{1..9}_or/file{1..9}_or' % m_point)
        ret, _, _ = g.run(self.clients[1], cmd)
        self.assertEqual(ret, 0, "Fail: Not able to create dir with files on "
                         "{}".format(m_point))
        g.log.info("Dirs with files are created successfully on mount point")

        # Create required soft links and hard links from client 1 on mount
        client, path = self.clients[0], m_point
        self.create_links(client, path)

        client = self.clients[1]
        for i in range(1, 10):

            # Create required soft and hard links in nested dirs
            path = '{}/dir{}_or'.format(m_point, i)
            self.create_links(client, path)

        # Create soft links for dirs
        path = m_point
        for i in range(4, 7):
            ret = create_link_file(client,
                                   '{}/dir{}_or'.format(path, i),
                                   '{}/dir{}_sl'.format(path, i), soft=True)
            self.assertTrue(ret, "Fail: Not able to create soft link for "
                            "{}/dir{}_or".format(path, i))
        g.log.info("Created nested soft and hard links for files successfully")

        # Calculate all file count against each section orginal, hard, soft
        # links
        cmd = ('cd %s; arr=(or sl hl); '
               'for i in ${arr[*]}; do find . -name "*$i" | wc -l ; '
               'done; ' % m_point)
        ret, out, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, "Not able get list of soft and hard links "
                         "created on the mount point")
        all_org, all_soft, all_hard = out.split()

        # Rename 2 out of 3 dir's soft links from client 1
        client = self.clients[0]
        cmd = ('cd %s; sl=0; '
               'for line in `ls -R | grep -P "dir(4|5)_sl"`; '
               'do mv -f "$line" "$line""_renamed"; ((sl++)); done; '
               'echo $sl;' % m_point)
        ret, out, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, "Not able to rename directory soft links")
        temp_soft = out.strip()

        # Start renaming original files from client 1 and
        # softlinks, hardlinks  from client 2
        cmd = ('cd %s; arr=(. dir{1..9}_or);  or=0; '
               'for item in ${arr[*]}; do '
               'cd $item; '
               'for line in `ls | grep -P "file(1|2)_or"`; '
               'do mv -f "$line" "$line""_renamed"; ((or++)); sleep 2; done;'
               'cd - > /dev/null; sleep 1; done; echo $or ' % m_point)
        proc_or = g.run_async(client, cmd)

        client = self.clients[1]
        cmd = ('cd %s; arr=(. dir{1..9}_or); sl=0; hl=0; '
               'for item in ${arr[*]}; do '
               'cd $item; '
               'for line in `ls | grep -P "file(4|5)_sl"`; '
               'do mv -f "$line" "$line""_renamed"; ((sl++)); sleep 1; done; '
               'for line in `ls | grep -P "file(7|8)_hl"`; '
               'do mv -f "$line" "$line""_renamed"; ((hl++)); sleep 1; done; '
               'cd - > /dev/null; sleep 1; done; echo $sl $hl; ' % m_point)
        proc_sl_hl = g.run_async(client, cmd)

        # Wait for some files to be renamed
        sleep(20)

        # Kill one of the bricks
        brick_list = get_all_bricks(self.mnode, self.volname)
        ret = bring_bricks_offline(self.volname, choice(brick_list))
        self.assertTrue(ret, "Failed to bring one of the bricks offline")

        # Wait for some more files to be renamed
        sleep(20)

        # Bring brick online
        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, "Not able to start Volume with force option")

        # Wait for rename to complete and take count of file operations
        ret, out, _ = proc_or.async_communicate()
        self.assertEqual(ret, 0, "Fail: Origianl files are not renamed")
        ren_org = out.strip()

        ret, out, _ = proc_sl_hl.async_communicate()
        self.assertEqual(ret, 0, "Fail: Soft and Hard links are not renamed")
        ren_soft, ren_hard = out.strip().split()
        ren_soft = str(int(ren_soft) + int(temp_soft))

        # Count actual data of renaming links/files
        cmd = ('cd %s; arr=(or or_renamed sl sl_renamed hl hl_renamed); '
               'for i in ${arr[*]}; do find . -name "*$i" | wc -l ; '
               'done; ' % m_point)
        ret, out, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, "Not able to get count of original and link "
                         "files after brick was brought up")
        (act_org, act_org_ren, act_soft,
         act_soft_ren, act_hard, act_hard_ren) = out.split()

        # Validate count of expected and actual rename of
        # links/files is matching
        for exp, act, msg in ((ren_org, act_org_ren, 'original'),
                              (ren_soft, act_soft_ren, 'soft links'),
                              (ren_hard, act_hard_ren, 'hard links')):
            self.assertEqual(exp, act, "Count of {} files renamed while brick "
                             "was offline is not matching".format(msg))

        # Validate no data is lost in rename process
        for exp, act, msg in (
                (int(all_org)-int(act_org_ren), int(act_org), 'original'),
                (int(all_soft)-int(act_soft_ren), int(act_soft), 'soft links'),
                (int(all_hard)-int(act_hard_ren), int(act_hard), 'hard links'),
        ):
            self.assertEqual(exp, act, "Count of {} files which are not "
                             "renamed while brick was offline "
                             "is not matching".format(msg))
