#  Copyright (C) 2020-2021  Red Hat, Inc. <http://www.redhat.com>
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

# pylint: disable=too-many-statements, too-many-locals

from unittest import SkipTest
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import get_dir_contents


@runs_on([['arbiter', 'distributed-replicated', 'distributed-dispersed'],
          ['glusterfs', 'cifs']])
class ValidateSosreportBehavior(GlusterBaseClass):
    """
    This testcase validates sosreport behavior with glusterfs
    """
    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=[self.mounts[0]],
                                                 volume_create_force=False)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")

    def tearDown(self):
        """tearDown"""
        ret = self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        self.get_super_method(self, 'tearDown')()

    def test_sosreport_behavior_for_glusterfs(self):
        '''
        Test Steps:
        1) Download sos package if not installed
        2) Fetch Sos version for reference
        3) Note down all files in below locations before taking sosreport:
            a) /var/run/gluster
            b) /run/gluster
            c) /var/lib/glusterd
            d) /var/log/glusterfs
        4) Take the sosreport
        5) Again note down the list of all gluster file in locations mentioned
        in step#3. The list of files in this step should match step#3
        6) untar the sosreport to see if gluster files are packaged
        '''

        # Fetching sosreport version for information
        ret, version, _ = g.run(self.servers[1], 'rpm -qa|grep sos')
        if version[4:9] in ('3.8-6', '3.8-7', '3.8-8'):
            raise SkipTest("Skipping testcase as bug is fixed in "
                           "sosreport version 3.8.9")
        g.log.info("sos version is %s", version)

        # Noting down list of entries in gluster directories before sos
        gluster_contents_before_sos = []
        gluster_dirs = ('/var/run/gluster*', '/run/gluster*',
                        '/var/lib/glusterd', '/var/log/glusterfs')
        for gdir in gluster_dirs:
            ret = get_dir_contents(self.servers[1], gdir, recursive=True)
            gluster_contents_before_sos.append(ret)

        # Check for any existing sosreport
        var_tmp_dircontents_before_sos = get_dir_contents(self.servers[1],
                                                          '/var/tmp/')

        # Collect sosreport
        ret, _, err = g.run(self.servers[1],
                            'sosreport --batch --name=$HOSTNAME')
        self.assertEqual(ret, 0, "failed to fetch sosreport due to {}"
                         .format(err))

        # Checking /var/tmp contents
        var_tmp_dircontents_after_sos = get_dir_contents(self.servers[1],
                                                         '/var/tmp/')

        # Recheck if all gluster files still exist
        gluster_contents_after_sos = []
        for gdir in gluster_dirs:
            ret = get_dir_contents(self.servers[1], gdir, recursive=True)
            gluster_contents_after_sos.append(ret)

        # Compare glusterfiles before and after taking sosreport
        # There should be no difference in contents
        # Ignoring /var/log/glusterfs ie last element of the list, to avoid
        # false negatives as sosreport triggers heal which creates new logs
        # and obvious difference in list of entries post sos
        self.assertTrue((gluster_contents_before_sos[:-1] ==
                         gluster_contents_after_sos[:-1]),
                        "Gluster files not matching before and after "
                        " sosreport generation {} and {}"
                        .format(gluster_contents_before_sos,
                                gluster_contents_after_sos))

        # Untar sosreport to check if gluster files are captured
        sosfile = list(set(var_tmp_dircontents_after_sos) -
                       set(var_tmp_dircontents_before_sos))
        sosfile.sort()
        untar_sosfile_cmd = 'tar -xvf /var/tmp/' + sosfile[0] + ' -C /var/tmp/'
        ret, _, err = g.run(self.servers[1], untar_sosfile_cmd)
        self.assertEqual(ret, 0, "Untar failed due to {}".format(err))
        dirchecks = ('/var/lib/glusterd', '/var/log/glusterfs')
        olddirs = [gluster_contents_after_sos[2],
                   gluster_contents_after_sos[3]]
        ret = {}
        for after, before in zip(dirchecks, olddirs):
            untar_dirpath = '/var/tmp/' + sosfile[0][0:-7]
            untardir = untar_dirpath + after
            _ = get_dir_contents(self.servers[1], untardir, recursive=True)
            ret[after] = list(x.split(untar_dirpath, 1)[-1] for x in _)
            if before == gluster_contents_after_sos[2]:
                difference = set(before)-set(ret[after])
                self.assertEqual(len(difference), 0,
                                 'gluster sosreport may be missing as they '
                                 'dont match with actual contents')
            else:
                # Need this logic for var/log/glusterfs entries as rotated(.gz)
                # logs are not collected by sos
                self.assertTrue(all(entry in before for entry in ret[after]),
                                'var-log-glusterfs entries in sosreport may be'
                                ' missing as they dont match with actual '
                                'contents')
