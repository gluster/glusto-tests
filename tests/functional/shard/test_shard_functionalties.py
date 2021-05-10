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

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterfile import (calculate_hash, get_fattr)
from glustolibs.gluster.brick_libs import (bring_bricks_offline, get_all_bricks)
from glustolibs.gluster.brickdir import BrickDir
from glustolibs.gluster.volume_libs import (get_subvols, set_volume_options, get_volume_options)


@runs_on([['distributed'],
          ['glusterfs']])
class TestShardFunctionalities(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUp
        cls.get_super_method(cls, 'setUpClass')()

        # Setup Volume and Mount Volume
        ret = cls.setup_volume_and_mount_volume(cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        # Enable sharding ON and set shard-block-size to 4MB                                  
        options = {'features.shard': 'on'}                             
        ret = set_volume_options(cls.mnode, cls.volname, options)

    @classmethod
    def tearDownClass(cls):

        # Unmount and cleanup original volume
        ret = cls.unmount_volume_and_cleanup_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        cls.get_super_method(cls, 'tearDownClass')()

    '''                                                                         
    test case: (shard file creation)                                            
        - Verify that the file is sharded and stored under $BRICK_PATH/.shard   
    '''
    def test_shard_create_file(self):
        '''
        Test file creation.
        '''
        mount_obj = self.mounts[0]
        mountpoint = mount_obj.mountpoint

        # verify sharding xlator is enabled                                  
        option = 'features.shard' 
        vol_option = get_volume_options(self.mnode, self.volname, option)
        self.assertEqual(vol_option['features.shard'], 'on', "Failed"         
                         " to validate "                                        
                         "volume options")                                      

        # verify default shard block size set to 64MB
        option = 'features.shard-block-size'
        vol_option = get_volume_options(self.mnode, self.volname, option)
        g.log.info("vinayk: vol_option = %s", vol_option['features.shard-block-size'])
        self.assertEqual(vol_option['features.shard-block-size'], '64MB', "Failed"           
                         " to validate "                                           
                         "volume options")                                         

        # set shard-block-size to 4MB                                  
        options = {'features.shard-block-size' : '4MB'}                             
        ret = set_volume_options(self.mnode, self.volname, options)

        # verify shard block size set to 4MB                           
        option = 'features.shard-block-size'                                                  
        vol_option = get_volume_options(self.mnode, self.volname, option)
        self.assertEqual(vol_option['features.shard-block-size'], '4MB', "Failed"           
                         " to validate "                                           
                         "volume options")                                         
        g.log.info("Successfully validated volume options"                      
                   "for volume %s", self.volname)

        # Creating a file on the mount-point                                    
        cmd = 'dd if=/dev/urandom of={}/file1 count=1M bs=10'.format(mountpoint)                                                               
        ret, _, _ = g.run(self.clients[0], cmd)                                    
        self.assertEqual(ret, 0, "Failed to create file")

        bricks_list = get_all_bricks(self.mnode, self.volname)                     

        # get the gfid of the file1
        gfid = ""
        for brick in bricks_list:                                                  
            brick_node, brick_path = brick.split(":")                           
            gfid = get_fattr(brick_node, brick_path + '/file1', "trusted.gfid")
            if gfid is not None:
                break

        count = 0
        # search for the shards of the file1
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")                                                  
            shard_dir = brick_path + '/.shard'
            cmd = ("ls -l %s | grep %s | wc -l" %(brick_path + '/.shard', gfid[2:] + '*')) 
            ret, out, _ = g.run(brick_node, cmd)
            count += int(out.strip())

        self.assertEqual(2, count, "Expected 2 shards but got {} ".format(count))

        g.log.info("Successfully validated file/shards creation")


