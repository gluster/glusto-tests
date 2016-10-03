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
                        cls.volume = volume
                        found_volume = True
                        break

        if found_volume:
            if not 'name' in cls.volume:
                cls.volume['name'] = 'testvol_%s' % cls.volume_type

            if 'servers' in cls.volume:
                cls.volume['servers'] = g.config['servers']

        if not found_volume:
            cls.volume = {
                'name': ('testvol_%s' % cls.volume_type),
                'servers': g.config['servers']
                }
            try:
                if g.config['gluster']['volume_types'][cls.volume_type]:
                    cls.volume['voltype'] = (g.config['gluster']
                                             ['volume_types'][cls.volume_type])
            except KeyError as e:
                try:
                    cls.volume['voltype'] = (default_volume_type_config
                                             [cls.volume_type])
                except KeyError as e:
                    g.log.error("Unable to get configs of volume type: %s",
                                cls.volume_type)
                    return False

        # SMB Info
        if cls.mount_type == 'cifs' or cls.mount_type == 'smb':
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
                        if ('volname' not in mount or (not mount['volname'])):
                            mount['volname'] = cls.volname
                        if ('server' not in mount or (not mount['server'])):
                            mount['server'] = mnode
                        if ('mountpoint' not in mount or
                                (not mount['mountpoint'])):
                            mount['mountpoint'] = (os.path.join(
                                "/mnt", '_'.join([cls.volname, cls.mount_type])))
                        cls.mounts_dict_list.append(mount)
                        found_mount = True
        if not found_mount:
            for client in g.config['clients']:
                mount = {
                    'protocol': cls.mount_type,
                    'server': cls.mnode,
                    'volname': cls.volname,
                    'client': {
                        'host': client
                        },
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

        # Get clients
        cls.clients = []
        if 'clients' in g.config:
            cls.clients = g.config['clients']
        else:
            for mount_dict in cls.mounts_dict_list:
                if 'client' in mount_dict:
                    if ('host' in mount_dict['client'] and
                            mount_dict['client']['host']):
                        if mount_dict['client']['host'] not in cls.clients:
                            cls.clients.append(mount_dict['client']['host'])

        # All servers info
        cls.all_servers_info = None
        if 'servers_info' in g.config:
            cls.all_servers_info = g.config['servers_info']
        else:
            g.log.error("servers_info not defined in the configuration file")

        # All clients_info
        cls.all_clients_info = None
        if 'clients_info' in g.config:
            cls.all_clients_info = g.config['clients_info']
        else:
            g.log.error("clients_info not defined in the configuration file")


class GlusterVolumeBaseClass(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        GlusterBaseClass.setUpClass.im_func(cls)

        # Start Glusterd
        from glustolibs.gluster.gluster_init import start_glusterd
        ret = start_glusterd(servers=cls.servers)
        if not ret:
            g.log.error("glusterd did not start on at least one server")
            return False

        # PeerProbe servers
        from glustolibs.gluster.peer_ops import peer_probe_servers
        ret = peer_probe_servers(mnode=cls.servers[0], servers=cls.servers[1:])
        if not ret:
            g.log.error("Unable to peer probe one or more servers")
            return False

        from glustolibs.gluster.volume_libs import setup_volume
        ret = setup_volume(mnode=cls.mnode,
                           all_servers_info=cls.all_servers_info,
                           volume_config=cls.volume)
        if not ret:
            g.log.error("Setup volume %s failed" % cls.volname)
            return False

        # Create Mounts
        for mount_obj in cls.mounts:
            ret = mount_obj.mount()
            if not ret:
                g.log.error("Unable to mount volume '%s:%s' on '%s:%s'" %
                            (mount_obj.server_system, mount_obj.volname,
                             mount_obj.client_system, mount_obj.mountpoint))
                return False

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
                                 mount_obj.client_system, mount_obj.mountpoint))
                    rc = False
            if not rc:
                return False

        # Cleanup volume
        if cleanup_vol:
            from glustolibs.gluster.volume_libs import cleanup_volume
            ret = cleanup_volume(mnode=cls.mnode, volname=cls.volname)
            if not ret:
                g.log.error("cleanup volume %s failed" % cls.volname)
                return False

        g.log.info("TEARDOWN GLUSTER VOLUME: %s on %s" % (cls.volume_type,
                                                          cls.mount_type))
