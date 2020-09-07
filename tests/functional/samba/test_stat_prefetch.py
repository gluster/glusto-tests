#  Copyright (C) 2017-2020  Red Hat, Inc. <http://www.redhat.com>
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

"""
Description:
        Pre-requisite : Have samba-ctd setup ready.
        Mount cifs and run certain IOs and meanwhile
        set stat-prefetch off-on this should fail the
        IO running on the mount point.
"""
from glusto.core import Glusto as g

from glustolibs.gluster.mount_ops import mount_volume
from glustolibs.gluster.exceptions import (ExecutionError)
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (set_volume_options,
                                           get_volume_options)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (validate_io_procs)


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['cifs']])
class TestValidateCifs(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        """
        setup volume and initialize necessary variables
        """

        cls.get_super_method(cls, 'setUpClass')()
        g.log.info("Starting %s:", cls.__name__)
        # Setup volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = cls.setup_volume()
        if not ret:
            raise ExecutionError("Failed to Setup_Volume")
        g.log.info("Successful in Setup Volume")

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to "
                                 "clients %s" % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the volume & mount
        """
        # stopping the volume and clean up the volume
        g.log.info("Starting to Cleanup Volume")
        ret = cls.unmount_volume_and_cleanup_volume(cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume and mount")
        g.log.info("Successful in Cleanup Volume and mount")

        # calling GlusterBaseClass tearDownClass
        cls.get_super_method(cls, 'tearDownClass')()

    def test_stat_prefetch(self):

        # pylint: disable=ungrouped-imports
        self.vips = (g.config['gluster']['cluster_config']['smb']['ctdb_vips'])
        # Virtual Ip of first node to mount
        self.vips_mnode = self.vips[0]['vip']
        g.log.info("CTDB Virtual Ip %s", self.vips_mnode)
        # run IOs
        self.counter = 1
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            ret, _, _ = mount_volume(self.volname, 'cifs',
                                     mount_obj.mountpoint,
                                     self.vips_mnode,
                                     mount_obj.client_system,
                                     smbuser='root', smbpasswd='foobar')
            self.assertEqual(ret, 0, "Cifs Mount Failed")
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_files -f 10000"
                   " --base-file-name ctdb-cifs "
                   " --fixed-file-size 10k %s/samba/"
                   % (self.script_upload_path,
                      mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
        self.io_validation_complete = False
        # Switch off and switch on stat-prefetch
        options = {"stat-prefetch": "off"}
        ret = set_volume_options(self.mnode, self.volname, options)
        if not ret:
            raise ExecutionError("Failed to execute volume set"
                                 "option command")
        ret = get_volume_options(self.mnode, self.volname)
        if ret['performance.stat-prefetch'] != "off":
            raise ExecutionError("Failed to set stat-prefetch off")
        options = {"stat-prefetch": "on"}
        ret = set_volume_options(self.mnode, self.volname, options)
        if not ret:
            raise ExecutionError("Failed to execute volume set"
                                 "option command")
        ret = get_volume_options(self.mnode, self.volname)
        if ret['performance.stat-prefetch'] != "on":
            raise ExecutionError("Failed to set stat-prefetch on")
        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Creation of 10000 files Success")
        g.log.info("test__samba_ctdb_cifs_io_rename PASSED")
