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
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import git_clone_and_compile
from glustolibs.gluster.volume_ops import set_volume_options


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed', 'arbiter', 'distributed-arbiter'],
          ['glusterfs']])
class TestGitCloneOnGlusterVolume(GlusterBaseClass):

    def setUp(self):
        self.get_super_method(self, 'setUp')()

        # Setup volume and mount it on one client
        if not self.setup_volume_and_mount_volume([self.mounts[0]]):
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        self.get_super_method(self, 'tearDown')()

        # Unmount from the one client and cleanup the volume
        if not self.unmount_volume_and_cleanup_volume([self.mounts[0]]):
            raise ExecutionError("Unable to unmount and cleanup volume")
        g.log.info("Unmount and volume cleanup is successful")

    def _run_git_clone(self, options):
        """Run git clone on the client"""

        repo = 'https://github.com/gluster/glusterfs.git'
        cloned_repo_dir = (self.mounts[0].mountpoint + '/' +
                           repo.split('/')[-1].rstrip('.git'))
        if options:
            cloned_repo_dir = (self.mounts[0].mountpoint + '/' + "perf-" +
                               repo.split('/')[-1].rstrip('.git'))
        ret = git_clone_and_compile(self.mounts[0].client_system,
                                    repo, cloned_repo_dir, False)
        self.assertTrue(ret, "Unable to clone {} repo on {}".
                        format(repo, cloned_repo_dir))
        g.log.info("Repo %s cloned successfully ", repo)

    def test_git_clone_on_gluster_volume(self):
        """
        Test Steps:
        1. Create a volume and mount it on one client
        2. git clone the glusterfs repo on the glusterfs volume.
        3. Set the performance options to off
        4. Repeat step 2 on a different directory.
        """
        self._run_git_clone(False)

        # Disable the performance cache options on the volume
        self.options = {'performance.quick-read': 'off',
                        'performance.stat-prefetch': 'off',
                        'performance.open-behind': 'off',
                        'performance.write-behind': 'off',
                        'performance.client-io-threads': 'off'}
        ret = set_volume_options(self.mnode, self.volname, self.options)
        self.assertTrue(ret, "Unable to set the volume options")
        g.log.info("Volume options set successfully")

        self._run_git_clone(True)
