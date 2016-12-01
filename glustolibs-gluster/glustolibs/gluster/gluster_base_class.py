#!/usr/bin/env python
#  Copyright (C) 2016 Red Hat, Inc. <http://www.redhat.com>
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
#
"""
    Description: Module containing GlusterBaseClass which defines all the
        variables necessary for tests.
"""

import unittest

from glusto.core import Glusto as g
import os
import random
from glustolibs.gluster.peer_ops import (is_peer_connected,
                                         peer_status)
from glustolibs.gluster.volume_libs import setup_volume, cleanup_volume
from glustolibs.gluster.volume_ops import volume_info, volume_status
import time
import copy


class runs_on(g.CarteTestClass):
    """Decorator providing runs_on capability for standard unittest script"""

    def __init__(self, value):
        # the names of the class attributes set by the runs_on decorator
        self.axis_names = ['volume_type', 'mount_type']

        # the options to replace 'ALL' in selections
        self.available_options = [['distributed', 'replicated',
                                   'distributed-replicated',
                                   'dispersed', 'distributed-dispersed'],
                                  ['glusterfs', 'nfs', 'cifs', 'smb']]

        # these are the volume and mount options to run and set in config
        # what do runs_on_volumes and runs_on_mounts need to be named????
        run_on_volumes = g.config.get('running_on_volumes',
                                      self.available_options[0])
        run_on_mounts = g.config.get('running_on_mounts',
                                     self.available_options[1])

        # selections is the above info from the run that is intersected with
        # the limits from the test script
        self.selections = [run_on_volumes, run_on_mounts]

        # value is the limits that are passed in by the decorator
        self.limits = value


class GlusterBaseClass(unittest.TestCase):
    # these will be populated by either the runs_on decorator or
    # defaults in setUpClass()
    volume_type = None
    mount_type = None
    volname = None
    servers = None
    voltype = None
    mnode = None
    mounts = None
    clients = None

    @classmethod
    def setUpClass(cls):
        """Initialize all the variables necessary for testing Gluster
        """
        # Get all servers
        cls.all_servers = None
        if ('servers' in g.config and g.config['servers']):
            cls.all_servers = g.config['servers']
        else:
            assert False, "'servers' not defined in the global config"

        # Get all clients
        cls.all_clients = None
        if ('clients' in g.config and g.config['clients']):
            cls.all_clients = g.config['clients']
        else:
            assert False, "'clients' not defined in the global config"

        # Get all servers info
        cls.all_servers_info = None
        if ('servers_info' in g.config and g.config['servers_info']):
            cls.all_servers_info = g.config['servers_info']
        else:
            assert False, "'servers_info' not defined in the global config"

        # All clients_info
        cls.all_clients_info = None
        if ('clients_info' in g.config and g.config['clients_info']):
            cls.all_clients_info = g.config['clients_info']
        else:
            assert False, "'clients_info' not defined in the global config"

        if cls.volume_type is None:
            cls.volume_type = "distributed"
        if cls.mount_type is None:
            cls.mount_type = "glusterfs"

        g.log.info("SETUP GLUSTER VOLUME: %s on %s" % (cls.volume_type,
                                                       cls.mount_type))

        # Defining default volume_types configuration.
        default_volume_type_config = {
            'replicated': {
                'type': 'replicated',
                'replica_count': 3,
                'transport': 'tcp'
                },
            'dispersed': {
                'type': 'dispersed',
                'disperse_count': 6,
                'redundancy_count': 2,
                'transport': 'tcp'
                },
            'distributed': {
                'type': 'distributed',
                'dist_count': 4,
                'transport': 'tcp'
                },
            'distributed-replicated': {
                'type': 'distributed-replicated',
                'dist_count': 2,
                'replica_count': 3,
                'transport': 'tcp'
                },
            'distributed-dispersed': {
                'type': 'distributed-dispersed',
                'dist_count': 2,
                'disperse_count': 6,
                'redundancy_count': 2,
                'transport': 'tcp'
                }
            }

        # Get the volume configuration.
        cls.volume = {}
        found_volume = False
        if 'gluster' in g.config:
            if 'volumes' in g.config['gluster']:
                for volume in g.config['gluster']['volumes']:
                    if volume['voltype']['type'] == cls.volume_type:
                        cls.volume = copy.deepcopy(volume)
                        found_volume = True
                        break

        if found_volume:
            if 'name' not in cls.volume:
                cls.volume['name'] = 'testvol_%s' % cls.volume_type

            if 'servers' not in cls.volume:
                cls.volume['servers'] = cls.all_servers

        if not found_volume:
            cls.volume = {
                'name': ('testvol_%s' % cls.volume_type),
                'servers': cls.all_servers
                }
            try:
                if g.config['gluster']['volume_types'][cls.volume_type]:
                    cls.volume['voltype'] = (g.config['gluster']
                                             ['volume_types'][cls.volume_type])
            except KeyError:
                try:
                    cls.volume['voltype'] = (default_volume_type_config
                                             [cls.volume_type])
                except KeyError:
                    assert False, ("Unable to get configs of volume type: %s",
                                   cls.volume_type)

        # Set volume options
        if 'options' not in cls.volume:
            cls.volume['options'] = {}

        # Set nfs.disable to 'off' to start gluster-nfs server on start of the
        # volume if the mount type is 'nfs'
        if cls.mount_type == 'nfs':
            cls.volume['options']['nfs.disable'] = 'off'

        # SMB Info
        if cls.mount_type == 'cifs' or cls.mount_type == 'smb':
            if 'smb' not in cls.volume:
                cls.volume['smb'] = {}
            cls.volume['smb']['enable'] = True
            users_info_found = False
            try:
                if cls.volume['smb']['users_info']:
                    users_info_found = True
            except KeyError:
                users_info_found = False

            if not users_info_found:
                cls.volume['smb']['users_info'] = {}
                try:
                    cls.volume['smb']['users_info'] = (
                        g.config['gluster']['cluster_config']['smb']
                        ['users_info'])
                except KeyError:
                    pass

                if not cls.volume['smb']['users_info']:
                    cls.volume['smb']['users_info']['root'] = {}
                    cls.volume['smb']['users_info']['root']['password'] = (
                        'foobar')

        # Define Volume variables.
        cls.volname = cls.volume['name']
        cls.servers = cls.volume['servers']
        cls.voltype = cls.volume['voltype']['type']
        cls.mnode = cls.servers[0]
        try:
            cls.smb_users_info = cls.volume['smb']['users_info']
        except KeyError:
            cls.smb_users_info = {}

        # Get the mount configuration.
        cls.mounts_dict_list = []
        cls.mounts = []
        found_mount = False
        if 'gluster' in g.config:
            if 'mounts' in g.config['gluster']:
                for mount in g.config['gluster']['mounts']:
                    if mount['protocol'] == cls.mount_type:
                        temp_mount = {}
                        temp_mount['protocol'] = cls.mount_type
                        if ('volname' in mount and mount['volname']):
                            if mount['volname'] == cls.volname:
                                temp_mount = copy.deepcopy(mount)
                            else:
                                continue
                        else:
                            temp_mount['volname'] = cls.volname
                        if ('server' not in temp_mount or
                                (not temp_mount['server'])):
                            temp_mount['server'] = cls.mnode
                        if ('mountpoint' not in temp_mount or
                                (not temp_mount['mountpoint'])):
                            temp_mount['mountpoint'] = (os.path.join(
                                "/mnt", '_'.join([cls.volname,
                                                  cls.mount_type])))
                        if ('client' not in temp_mount or
                                (not temp_mount['client'])):
                            temp_mount['client'] = (
                                cls.all_clients_info[
                                    random.choice(cls.all_clients_info.keys())]
                                )
                        cls.mounts_dict_list.append(temp_mount)
                        found_mount = True
        if not found_mount:
            for client in cls.all_clients_info.keys():
                mount = {
                    'protocol': cls.mount_type,
                    'server': cls.mnode,
                    'volname': cls.volname,
                    'client': cls.all_clients_info[client],
                    'mountpoint': (os.path.join(
                        "/mnt", '_'.join([cls.volname, cls.mount_type]))),
                    'options': ''
                    }
                cls.mounts_dict_list.append(mount)

        if cls.mount_type == 'cifs' or cls.mount_type == 'smb':
            for mount in cls.mounts_dict_list:
                if 'smbuser' not in mount:
                    mount['smbuser'] = random.choice(cls.smb_users_info.keys())
                    mount['smbpasswd'] = (
                        cls.smb_users_info[mount['smbuser']]['password'])

        from glustolibs.gluster.mount_ops import create_mount_objs
        cls.mounts = create_mount_objs(cls.mounts_dict_list)

        # Defining clients from mounts.
        cls.clients = []
        for mount_dict in cls.mounts_dict_list:
            cls.clients.append(mount_dict['client']['host'])
        cls.clients = list(set(cls.clients))


class GlusterVolumeBaseClass(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        GlusterBaseClass.setUpClass.im_func(cls)

        # Validate if peer is connected from all the servers
        for server in cls.servers:
            ret = is_peer_connected(server, cls.servers)
            assert (ret == True), "Validating Peers to be in Cluster Failed"

        # Print Peer Status from mnode
        _, _, _ = peer_status(cls.mnode)

        # Setup Volume
        ret = setup_volume(mnode=cls.mnode,
                           all_servers_info=cls.all_servers_info,
                           volume_config=cls.volume, force=True)
        assert (ret == True), "Setup volume %s failed" % cls.volname
        time.sleep(10)

        # Print Volume Info and Status
        _, _, _ = volume_info(cls.mnode, cls.volname)

        _, _, _ = volume_status(cls.mnode, cls.volname)

        # Validate if volume is exported or not
        if 'nfs' in cls.mount_type:
            cmd = "showmount -e localhost"
            _, _, _ = g.run(cls.mnode, cmd)

            cmd = "showmount -e localhost | grep %s" % cls.volname
            ret, _, _ = g.run(cls.mnode, cmd)
            assert (ret == 0), "Volume %s not exported" % cls.volname

        if 'cifs' in cls.mount_type:
            cmd = "smbclient -L localhost"
            _, _, _ = g.run(cls.mnode, cmd)

            cmd = ("smbclient -L localhost -U | grep -i -Fw gluster-%s " %
                   cls.volname)
            ret, _, _ = g.run(cls.mnode, cmd)
            assert (ret == 0), ("Volume %s not accessable via SMB/CIFS share" %
                                cls.volname)

        # Create Mounts
        rc = True
        for mount_obj in cls.mounts:
            ret = mount_obj.mount()
            if not ret:
                g.log.error("Unable to mount volume '%s:%s' on '%s:%s'" %
                            (mount_obj.server_system, mount_obj.volname,
                             mount_obj.client_system, mount_obj.mountpoint))
                rc = False
        assert (rc == True), ("Mounting volume %s on few clients failed" %
                              cls.volname)

    @classmethod
    def tearDownClass(cls, umount_vol=True, cleanup_vol=True):
        """unittest tearDownClass override"""
        # Unmount volume
        if umount_vol:
            rc = True
            for mount_obj in cls.mounts:
                ret = mount_obj.unmount()
                if not ret:
                    g.log.error("Unable to unmount volume '%s:%s' on '%s:%s'" %
                                (mount_obj.server_system, mount_obj.volname,
                                 mount_obj.client_system, mount_obj.mountpoint)
                                )
                    rc = False
            assert (rc == True), ("Unmount of all mounts are not successful")

        # Cleanup volume
        if cleanup_vol:
            ret = cleanup_volume(mnode=cls.mnode, volname=cls.volname)
            assert (ret == True), ("cleanup volume %s failed" % cls.volname)
