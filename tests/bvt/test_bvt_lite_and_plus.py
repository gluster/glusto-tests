#!/usr/bin/env python
#  Copyright (C) 2015-2016  Red Hat, Inc. <http://www.redhat.com>
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

import pytest
import os
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterVolumeBaseClass,
                                                   runs_on)
import time


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class BvtTestsClass(GlusterVolumeBaseClass):
    """Class containing case for : BVT Lite and BVT Plus.

    BVT Lite: Run the case on dis-rep volume with glusterfs, nfs, cifs
        protocols

    BVT Plus: Run the case on all volume types and all protocol types
        combinations
    """
    @classmethod
    def setUpClass(cls):
        """Setup Volume and Mounts.
        """
        g.log.info("Starting %s:" % cls.__name__)
        GlusterVolumeBaseClass.setUpClass.im_func(cls)

        # Upload io scripts
        cls.script_local_path = ("/usr/share/glustolibs/io/"
                                 "scripts/file_dir_ops.py")
        cls.script_upload_path = "/tmp/file_dir_ops.py"
        ret = os.path.exists(cls.script_local_path)
        assert (ret == True), ("Unable to find the io scripts")

        for client in cls.clients:
            g.upload(client, cls.script_local_path, cls.script_upload_path)
            g.run(client, "ls -l %s" % cls.script_upload_path)
            g.run(client, "chmod +x %s" % cls.script_upload_path)
            g.run(client, "ls -l %s" % cls.script_upload_path)

    def setUp(self):
        pass

    def test_bvt(self):
        """Test IO from the mounts.
        """
        g.log.info("Starting Test: %s on %s %s" %
                   (self.id(), self.volume_type, self.mount_type))

        # Get stat of mount before the IO
        for mount_obj in self.mounts:
            cmd = "mount | grep %s" % mount_obj.mountpoint
            ret, out, err = g.run(mount_obj.client_system, cmd)
            cmd = "df -h %s" % mount_obj.mountpoint
            ret, out, err = g.run(mount_obj.client_system, cmd)
            cmd = "ls -ld %s" % mount_obj.mountpoint
            ret, out, err = g.run(mount_obj.client_system, cmd)
            cmd = "stat %s" % mount_obj.mountpoint
            ret, out, err = g.run(mount_obj.client_system, cmd)

        # Start IO on all mounts.
        all_mounts_procs = []
        count = 1
        for mount_obj in self.mounts:
            cmd = ("python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (self.script_upload_path,
                                            count, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        # Get IO status
        rc = True
        for i, proc in enumerate(all_mounts_procs):
            ret, _, _ = proc.async_communicate()
            if ret != 0:
                g.log.error("IO Failed on %s:%s" %
                            (self.mounts[i].client_system,
                             self.mounts[i].mountpoint))
                rc = False
        assert (rc == True), "IO failed on some of the clients"

        # Get stat of all the files/dirs created.
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s stat "
                   "-R %s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
        rc = True
        for i, proc in enumerate(all_mounts_procs):
            ret, _, _ = proc.async_communicate()
            if ret != 0:
                g.log.error("Stat of files and dirs under %s:%s Failed" %
                            (self.mounts[i].client_system,
                             self.mounts[i].mountpoint))
                rc = False
        assert (rc == True), "Stat failed on some of the clients"

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        """Cleanup mount and Cleanup the volume
        """
        GlusterVolumeBaseClass.tearDownClass.im_func(cls)
