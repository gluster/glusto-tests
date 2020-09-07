#  Copyright (C) 2019  Red Hat, Inc. <http://www.redhat.com>
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

""" Description:
    This cthon test is specific to NFS Ganesha
    and runs on v.4.0, v4.1 clients.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import runs_on, GlusterBaseClass
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.io.utils import run_cthon
from glustolibs.misc.misc_libs import git_clone_and_compile


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestCthon(GlusterBaseClass):
    """
        Cthon test on NFS Ganesha v4.0, v4.1
    """

    @classmethod
    def setUpClass(cls):
        """
        Setup nfs-ganesha if not exists.
        """
        cls.get_super_method(cls, 'setUpClass')()

        # Cloning the cthon test repo
        cls.dir_name = "repo_dir"
        link = 'git://linux-nfs.org/~steved/cthon04.git'
        ret = git_clone_and_compile(cls.clients, link, cls.dir_name,
                                    compile_option=True)
        if not ret:
            raise ExecutionError("Failed to clone the cthon repo."
                                 "Check error logs to know which"
                                 "node it failed on.")
        else:
            g.log.info("Successfully cloned the"
                       "test repo on provided nodes.")

    def setUp(self):
        """
        Setup volume
        """
        self.get_super_method(self, 'setUp')()

        g.log.info("Starting to setup volume %s", self.volname)
        ret = self.setup_volume(volume_create_force=True)
        if not ret:
            raise ExecutionError("Failed to setup"
                                 "volume %s" % self.volname)
        g.log.info("Successful setup of volume %s" % self.volname)

    def test_NFS_cthon(self):
        """The cthon test is divied into four groups.
        Basic : basic file system operations tests
        General : general file system tests
        Special : tests that poke certain common problem areas
        Lock : tests that exercise network locking
        """
        ret = run_cthon(self.mnode, self.volname,
                        self.clients, self.dir_name)
        self.assertTrue(ret, ("Cthon test failed"))
        g.log.info("Cthon test successfully passed")

    def tearDown(self):
        """
        Cleanup volume
        """
        self.get_super_method(self, 'tearDown')()

        # Cleanup volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup volume")
        g.log.info("Cleanup of volume %s"
                   " completed successfully", self.volname)

        # Cleanup test repo
        flag = 0
        for client in self.clients:
            ret = g.run(client, "rm -rf /root/%s" % self.dir_name)
            if ret:
                g.log.error("Failed to cleanup test repo on "
                            "client %s" % client)
                flag = 1
            else:
                g.log.info("Test repo successfully cleaned on "
                           "client %s" % client)
        if flag:
            raise ExecutionError("Test repo failed. "
                                 "Check log errors for more info")
        else:
            g.log.info("Test repo cleanup successfull on all clients")
