#  Copyright (C) 2018 Red Hat, Inc. <http://www.redhat.com>
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
import os
import random
import copy
import datetime
import time
import socket
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ConfigError, ExecutionError
from glustolibs.gluster.peer_ops import is_peer_connected, peer_status
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.block_ops import block_delete
from glustolibs.gluster.block_libs import (setup_block, if_block_exists,
                                           get_block_list,
                                           get_block_info)
from glustolibs.gluster.volume_libs import (setup_volume,
                                            cleanup_volume,
                                            log_volume_info_and_status)
from glustolibs.gluster.volume_libs import (
    wait_for_volume_process_to_be_online)
from glustolibs.gluster.samba_libs import share_volume_over_smb
from glustolibs.gluster.nfs_libs import export_volume_through_nfs
from glustolibs.gluster.mount_ops import create_mount_objs
from glustolibs.io.utils import log_mounts_info
from glustolibs.gluster.lib_utils import inject_msg_in_logs


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
        run_on_volumes = self.available_options[0]
        run_on_mounts = self.available_options[1]
        if 'gluster' in g.config and g.config['gluster']:
            if ('running_on_volumes' in g.config['gluster'] and
                    g.config['gluster']['running_on_volumes']):
                run_on_volumes = g.config['gluster']['running_on_volumes']

            if ('running_on_mounts' in g.config['gluster'] and
                    g.config['gluster']['running_on_mounts']):
                run_on_mounts = g.config['gluster']['running_on_mounts']
        # selections is the above info from the run that is intersected with
        # the limits from the test script
        self.selections = [run_on_volumes, run_on_mounts]

        # value is the limits that are passed in by the decorator
        self.limits = value


class GlusterBaseClass(unittest.TestCase):
    """GlusterBaseClass to be subclassed by Gluster Tests.
    This class reads the config for variable values that will be used in
    gluster tests. If variable values are not specified in the config file,
    the variable are defaulted to specific values.
    """
    # these will be populated by either the runs_on decorator or
    # defaults in setUpClass()
    volume_type = None
    mount_type = None

    @classmethod
    def inject_msg_in_gluster_logs(cls, msg):
        """Inject all the gluster logs on servers, clients with msg

        Args:
            msg (str): Message string to be injected

        Returns:
            bool: True if injecting msg on the log files/dirs is successful.
                False Otherwise.
        """
        _rc = True
        # Inject msg on server gluster logs
        ret = inject_msg_in_logs(cls.servers, log_msg=msg,
                                 list_of_dirs=cls.server_gluster_logs_dirs,
                                 list_of_files=cls.server_gluster_logs_files)
        if not ret:
            _rc = False

        if cls.mount_type is not None and "glusterfs" in cls.mount_type:
            ret = inject_msg_in_logs(
                cls.clients, log_msg=msg,
                list_of_dirs=cls.client_gluster_logs_dirs,
                list_of_files=cls.client_gluster_logs_files)
            if not ret:
                _rc = False
        return _rc

    @classmethod
    def get_ip_from_hostname(cls, nodes):
        """Returns list of IP's for the list of nodes in order.

        Args:
            nodes(list): List of nodes hostnames

        Returns:
            list: List of IP's corresponding to the hostnames of nodes.
        """
        nodes_ips = []
        if isinstance(nodes, str):
            nodes = [nodes]
        for node in nodes:
            try:
                ip = socket.gethostbyname(node)
            except socket.gaierror as e:
                g.log.error("Failed to get the IP of Host: %s : %s", node,
                            e.strerror)
                ip = None
            nodes_ips.append(ip)
        return nodes_ips

    @classmethod
    def validate_peers_are_connected(cls):
        """Validate whether each server in the cluster is connected to
        all other servers in cluster.

        Returns (bool): True if all peers are in connected with other peers.
            False otherwise.
        """
        # Validate if peer is connected from all the servers
        g.log.info("Validating if servers %s are connected from other servers "
                   "in the cluster", cls.servers)
        for server in cls.servers:
            g.log.info("Validate servers %s are in connected from  node %s",
                       cls.servers, server)
            ret = is_peer_connected(server, cls.servers)
            if not ret:
                g.log.error("Some or all servers %s are not in connected "
                            "state from node %s", cls.servers, server)
                return False
            g.log.info("Successfully validated servers %s are all in "
                       "connected state from node %s",
                       cls.servers, server)
        g.log.info("Successfully validated all servers %s are in connected "
                   "state from other servers in the cluster", cls.servers)

        # Peer Status from mnode
        peer_status(cls.mnode)

        return True

    @classmethod
    def setup_volume(cls, volume_create_force=False):
        """Setup the volume:
            - Create the volume, Start volume, Set volume
            options, enable snapshot/quota/tier if specified in the config
            file.
            - Wait for volume processes to be online
            - Export volume as NFS/SMB share if mount_type is NFS or SMB
            - Log volume info and status

        Args:
            volume_create_force(bool): True if create_volume should be
                executed with 'force' option.

        Returns (bool): True if all the steps mentioned in the descriptions
            passes. False otherwise.
        """
        force_volume_create = False
        if volume_create_force or cls.volume_create_force:
            force_volume_create = True

        # Validate peers before setting up volume
        g.log.info("Validate peers before setting up volume ")
        ret = cls.validate_peers_are_connected()
        if not ret:
            g.log.error("Failed to validate peers are in connected state "
                        "before setting up volume")
            return False
        g.log.info("Successfully validated peers are in connected state "
                   "before setting up volume")

        # Setup Volume
        g.log.info("Setting up volume %s", cls.volname)
        ret = setup_volume(mnode=cls.mnode,
                           all_servers_info=cls.all_servers_info,
                           volume_config=cls.volume, force=force_volume_create)
        if not ret:
            g.log.error("Failed to Setup volume %s", cls.volname)
            return False
        g.log.info("Successful in setting up volume %s", cls.volname)

        # Wait for volume processes to be online
        g.log.info("Wait for volume %s processes to be online", cls.volname)
        ret = wait_for_volume_process_to_be_online(cls.mnode, cls.volname)
        if not ret:
            g.log.error("Failed to wait for volume %s processes to "
                        "be online", cls.volname)
            return False
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", cls.volname)

        # Export/Share the volume based on mount_type
        if cls.mount_type != "glusterfs":
            g.log.info("Export/Sharing the volume %s", cls.volname)
            if "nfs" in cls.mount_type:
                ret = export_volume_through_nfs(
                    mnode=cls.mnode, volname=cls.volname,
                    enable_ganesha=cls.enable_nfs_ganesha)
                if not ret:
                    g.log.error("Failed to export volume %s "
                                "as NFS export", cls.volname)
                    return False
                g.log.info("Successful in exporting the volume %s "
                           "as NFS export", cls.volname)

                # Set NFS-Ganesha specific volume options
                if cls.enable_nfs_ganesha and cls.nfs_ganesha_export_options:
                    g.log.info("Setting NFS-Ganesha export specific "
                               "volume options on volume %s", cls.volname)
                    ret = set_volume_options(
                        mnode=cls.mnode, volname=cls.volname,
                        options=cls.nfs_ganesha_export_options)
                    if not ret:
                        g.log.error("Failed to set NFS-Ganesha "
                                    "export specific options on "
                                    "volume %s", cls.volname)
                        return False
                    g.log.info("Successful in setting NFS-Ganesha export "
                               "specific volume options on volume %s",
                               cls.volname)

            if "smb" in cls.mount_type or "cifs" in cls.mount_type:
                ret = share_volume_over_smb(mnode=cls.mnode,
                                            volname=cls.volname,
                                            smb_users_info=cls.smb_users_info)
                if not ret:
                    g.log.error("Failed to export volume %s "
                                "as SMB Share", cls.volname)
                    return False
                g.log.info("Successful in exporting volume %s as SMB Share",
                           cls.volname)

                # Set SMB share specific volume options
                if cls.smb_share_options:
                    g.log.info("Setting SMB share specific volume options "
                               "on volume %s", cls.volname)
                    ret = set_volume_options(mnode=cls.mnode,
                                             volname=cls.volname,
                                             options=cls.smb_share_options)
                    if not ret:
                        g.log.error("Failed to set SMB share "
                                    "specific options "
                                    "on volume %s", cls.volname)
                        return False
                    g.log.info("Successful in setting SMB share specific "
                               "volume options on volume %s", cls.volname)

        # Log Volume Info and Status
        g.log.info("Log Volume %s Info and Status", cls.volname)
        ret = log_volume_info_and_status(cls.mnode, cls.volname)
        if not ret:
            g.log.error("Logging volume %s info and status failed",
                        cls.volname)
            return False
        g.log.info("Successful in logging volume %s info and status",
                   cls.volname)

        return True

    @classmethod
    def mount_volume(cls, mounts):
        """Mount volume

        Args:
            mounts(list): List of mount_objs

        Returns (bool): True if mounting the volume for a mount obj is
            successful. False otherwise
        """
        g.log.info("Starting to mount volume %s", cls.volname)
        for mount_obj in mounts:
            g.log.info("Mounting volume '%s:%s' on '%s:%s'",
                       mount_obj.server_system, mount_obj.volname,
                       mount_obj.client_system, mount_obj.mountpoint)
            ret = mount_obj.mount()
            if not ret:
                g.log.error("Failed to mount volume '%s:%s' on '%s:%s'",
                            mount_obj.server_system, mount_obj.volname,
                            mount_obj.client_system, mount_obj.mountpoint)
                return False
            else:
                g.log.info("Successful in mounting volume '%s:%s' on "
                           "'%s:%s'", mount_obj.server_system,
                           mount_obj.volname, mount_obj.client_system,
                           mount_obj.mountpoint)
        g.log.info("Successful in mounting all mount objs for the volume %s",
                   cls.volname)

        # Get mounts info
        g.log.info("Get mounts Info:")
        log_mounts_info(mounts)

        return True

    @classmethod
    def setup_volume_and_mount_volume(cls, mounts, volume_create_force=False):
        """Setup the volume and mount the volume

        Args:
            mounts(list): List of mount_objs
            volume_create_force(bool): True if create_volume should be
                executed with 'force' option.

        Returns (bool): True if setting up volume and mounting the volume
            for a mount obj is successful. False otherwise
        """
        # Setup Volume
        _rc = cls.setup_volume(volume_create_force)
        if not _rc:
            return _rc

        # Mount Volume
        _rc = cls.mount_volume(mounts)
        if not _rc:
            return _rc

        return True

    @classmethod
    def unmount_volume(cls, mounts):
        """Unmount all mounts for the volume

        Args:
            mounts(list): List of mount_objs

        Returns (bool): True if unmounting the volume for a mount obj is
            successful. False otherwise
        """
        # Unmount volume
        g.log.info("Starting to UnMount Volume %s", cls.volname)
        for mount_obj in mounts:
            g.log.info("UnMounting volume '%s:%s' on '%s:%s'",
                       mount_obj.server_system, mount_obj.volname,
                       mount_obj.client_system, mount_obj.mountpoint)
            ret = mount_obj.unmount()
            if not ret:
                g.log.error("Failed to unmount volume '%s:%s' on '%s:%s'",
                            mount_obj.server_system, mount_obj.volname,
                            mount_obj.client_system, mount_obj.mountpoint)

                # Get mounts info
                g.log.info("Get mounts Info:")
                log_mounts_info(cls.mounts)

                return False
            else:
                g.log.info("Successful in unmounting volume '%s:%s' on "
                           "'%s:%s'", mount_obj.server_system,
                           mount_obj.volname, mount_obj.client_system,
                           mount_obj.mountpoint)
        g.log.info("Successful in unmounting all mount objs for the volume %s",
                   cls.volname)

        # Get mounts info
        g.log.info("Get mounts Info:")
        log_mounts_info(mounts)

        return True

    @classmethod
    def cleanup_volume(cls):
        """Cleanup the volume

        Returns (bool): True if cleanup volume is successful. False otherwise.
        """
        g.log.info("Cleanup Volume %s", cls.volname)
        ret = cleanup_volume(mnode=cls.mnode, volname=cls.volname)
        if not ret:
            g.log.error("cleanup of volume %s failed", cls.volname)
        else:
            g.log.info("Successfully cleaned-up volume %s", cls.volname)

        # Log Volume Info and Status
        g.log.info("Log Volume %s Info and Status", cls.volname)
        log_volume_info_and_status(cls.mnode, cls.volname)

        return ret

    @classmethod
    def unmount_volume_and_cleanup_volume(cls, mounts):
        """Unmount the volume and cleanup volume

        Args:
            mounts(list): List of mount_objs

        Returns (bool): True if unmounting the volume for the mounts and
            cleaning up volume is successful. False otherwise
        """
        # UnMount Volume
        _rc = cls.unmount_volume(mounts)
        if not _rc:
            return _rc

        # Setup Volume
        _rc = cls.cleanup_volume()
        if not _rc:
            return _rc

        return True

    @classmethod
    def setUpClass(cls):
        """Initialize all the variables necessary for testing Gluster
        """
        # Get all servers
        cls.all_servers = None
        if 'servers' in g.config and g.config['servers']:
            cls.all_servers = g.config['servers']
            cls.servers = cls.all_servers
        else:
            raise ConfigError("'servers' not defined in the global config")

        # Get all slaves
        cls.slaves = None
        if 'slaves' in g.config and g.config['slaves']:
            cls.slaves = g.config['slaves']
            # Set mnode_slave : Node on which slave commands are executed
            cls.mnode_slave = cls.slaves[0]
            # Slave IP's
            cls.slaves_ip = []
            cls.slaves_ip = cls.get_ip_from_hostname(cls.slaves)

        # Get all clients
        cls.all_clients = None
        if 'clients' in g.config and g.config['clients']:
            cls.all_clients = g.config['clients']
            cls.clients = cls.all_clients
        else:
            raise ConfigError("'clients' not defined in the global config")

        # Get all servers info
        cls.all_servers_info = None
        if 'servers_info' in g.config and g.config['servers_info']:
            cls.all_servers_info = g.config['servers_info']
        else:
            raise ConfigError("'servers_info' not defined in the global "
                              "config")
        # Get all slaves info
        cls.all_slaves_info = None
        if 'slaves_info' in g.config and g.config['slaves_info']:
            cls.all_slaves_info = g.config['slaves_info']

        # All clients_info
        cls.all_clients_info = None
        if 'clients_info' in g.config and g.config['clients_info']:
            cls.all_clients_info = g.config['clients_info']
        else:
            raise ConfigError("'clients_info' not defined in the global "
                              "config")

        # Set mnode : Node on which gluster commands are executed
        cls.mnode = cls.all_servers[0]

        # Server IP's
        cls.servers_ips = []
        cls.servers_ips = cls.get_ip_from_hostname(cls.servers)

        # SMB Cluster info
        try:
            cls.smb_users_info = (
                g.config['gluster']['cluster_config']['smb']['users_info'])
        except KeyError:
            cls.smb_users_info = {}
            cls.smb_users_info['root'] = {}
            cls.smb_users_info['root']['password'] = 'foobar'
            cls.smb_users_info['root']['acl'] = 'rwx'

        # NFS-Ganesha Cluster info
        try:
            cls.enable_nfs_ganesha = bool(g.config['gluster']['cluster_config']
                                          ['nfs_ganesha']['enable'])
            cls.num_of_nfs_ganesha_nodes = (g.config['gluster']
                                            ['cluster_config']['nfs_ganesha']
                                            ['num_of_nfs_ganesha_nodes'])
            cls.vips = (g.config['gluster']['cluster_config']['nfs_ganesha']
                        ['vips'])
        except KeyError:
            cls.enable_nfs_ganesha = False
            cls.num_of_nfs_ganesha_nodes = None
            cls.vips = []

        # Defining default volume_types configuration.
        cls.default_volume_type_config = {
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

        # Check if default volume_type configuration is provided in
        # config yml
        if (g.config.get('gluster') and
                g.config['gluster'].get('volume_types')):
            default_volume_type_from_config = (
                g.config['gluster']['volume_types'])

            for volume_type in default_volume_type_from_config.keys():
                if default_volume_type_from_config[volume_type]:
                    if volume_type in cls.default_volume_type_config:
                        cls.default_volume_type_config[volume_type] = (
                            default_volume_type_from_config[volume_type])

        # Create Volume with force option
        cls.volume_create_force = False
        if (g.config.get('gluster') and
                g.config['gluster'].get('volume_create_force')):
            cls.volume_create_force = (
                g.config['gluster']['volume_create_force'])

        # Default volume options which is applicable for all the volumes
        cls.volume_options = {}
        if (g.config.get('gluster') and
                g.config['gluster'].get('volume_options')):
            cls.volume_options = g.config['gluster']['volume_options']

        # If the volume is exported as SMB Share, then set the following
        # volume options on the share.
        cls.smb_share_options = {}
        if (g.config.get('gluster') and
                g.config['gluster'].get('smb_share_options')):
            cls.smb_share_options = (
                g.config['gluster']['smb_share_options'])

        # If the volume is exported as NFS-Ganesha export,
        # then set the following volume options on the export.
        cls.nfs_ganesha_export_options = {}
        if (g.config.get('gluster') and
                g.config['gluster'].get('nfs_ganesha_export_options')):
            cls.nfs_ganesha_export_options = (
                g.config['gluster']['nfs_ganesha_export_options'])

        # Get the volume configuration.
        cls.volume = {}
        if cls.volume_type:
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
                try:
                    if g.config['gluster']['volume_types'][cls.volume_type]:
                        cls.volume['voltype'] = (g.config['gluster']
                                                 ['volume_types']
                                                 [cls.volume_type])
                except KeyError:
                    try:
                        cls.volume['voltype'] = (cls.default_volume_type_config
                                                 [cls.volume_type])
                    except KeyError:
                        raise ConfigError("Unable to get configs of volume "
                                          "type: %s", cls.volume_type)
                cls.volume['name'] = 'testvol_%s' % cls.volume_type
                cls.volume['servers'] = cls.all_servers

            # Set volume options
            if 'options' not in cls.volume:
                cls.volume['options'] = cls.volume_options

            # Define Volume Useful Variables.
            cls.volname = cls.volume['name']
            cls.voltype = cls.volume['voltype']['type']
            cls.servers = cls.volume['servers']
            cls.mnode = cls.servers[0]
            cls.vol_options = cls.volume['options']

        # Get the mount configuration.
        cls.mounts = []
        if cls.mount_type:
            cls.mounts_dict_list = []
            found_mount = False
            if 'gluster' in g.config:
                if 'mounts' in g.config['gluster']:
                    for mount in g.config['gluster']['mounts']:
                        if mount['protocol'] == cls.mount_type:
                            temp_mount = {}
                            temp_mount['protocol'] = cls.mount_type
                            if 'volname' in mount and mount['volname']:
                                if mount['volname'] == cls.volname:
                                    temp_mount = copy.deepcopy(mount)
                                else:
                                    continue
                            else:
                                temp_mount['volname'] = cls.volname
                            if ('server' not in mount or
                                    (not mount['server'])):
                                temp_mount['server'] = cls.mnode
                            else:
                                temp_mount['server'] = mount['server']
                            if ('mountpoint' not in mount or
                                    (not mount['mountpoint'])):
                                temp_mount['mountpoint'] = (os.path.join(
                                    "/mnt", '_'.join([cls.volname,
                                                      cls.mount_type])))
                            else:
                                temp_mount['mountpoint'] = mount['mountpoint']
                            if ('client' not in mount or
                                    (not mount['client'])):
                                temp_mount['client'] = (
                                    cls.all_clients_info[
                                        random.choice(
                                            cls.all_clients_info.keys())]
                                    )
                            else:
                                temp_mount['client'] = mount['client']
                            if 'options' in mount and mount['options']:
                                temp_mount['options'] = mount['options']
                            else:
                                temp_mount['options'] = ''
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
                        mount['smbuser'] = random.choice(
                            cls.smb_users_info.keys())
                        mount['smbpasswd'] = (
                            cls.smb_users_info[mount['smbuser']]['password'])

            cls.mounts = create_mount_objs(cls.mounts_dict_list)

            # Defining clients from mounts.
            cls.clients = []
            for mount in cls.mounts_dict_list:
                cls.clients.append(mount['client']['host'])
            cls.clients = list(set(cls.clients))

        # Gluster Logs info
        cls.server_gluster_logs_dirs = ["/var/log/glusterfs",
                                        "/var/log/samba"]
        cls.server_gluster_logs_files = ["/var/log/ganesha.log",
                                         "/var/log/ganesha-gfapi.log"]
        if ('gluster' in g.config and
                'server_gluster_logs_info' in g.config['gluster']):
            server_gluster_logs_info = (
                g.config['gluster']['server_gluster_logs_info'])
            if ('dirs' in server_gluster_logs_info and
                    server_gluster_logs_info['dirs']):
                cls.server_gluster_logs_dirs = (
                    server_gluster_logs_info['dirs'])

            if ('files' in server_gluster_logs_info and
                    server_gluster_logs_info['files']):
                cls.server_gluster_logs_files = (
                    server_gluster_logs_info['files'])

        cls.client_gluster_logs_dirs = ["/var/log/glusterfs"]
        cls.client_gluster_logs_files = []
        if ('gluster' in g.config and
                'client_gluster_logs_info' in g.config['gluster']):
            client_gluster_logs_info = (
                g.config['gluster']['client_gluster_logs_info'])
            if ('dirs' in client_gluster_logs_info and
                    client_gluster_logs_info['dirs']):
                cls.client_gluster_logs_dirs = (
                    client_gluster_logs_info['dirs'])

            if ('files' in client_gluster_logs_info and
                    client_gluster_logs_info['files']):
                cls.client_gluster_logs_files = (
                    client_gluster_logs_info['files'])

        # Have a unique string to recognize the test run for logging in
        # gluster logs
        if 'glustotest_run_id' not in g.config:
            g.config['glustotest_run_id'] = (
                datetime.datetime.now().strftime('%H_%M_%d_%m_%Y'))
        cls.glustotest_run_id = g.config['glustotest_run_id']

        msg = "Setupclass: %s : %s" % (cls.__name__, cls.glustotest_run_id)
        g.log.info(msg)
        cls.inject_msg_in_gluster_logs(msg)

        # Log the baseclass variables for debugging purposes
        g.log.debug("GlusterBaseClass Variables:\n %s", cls.__dict__)

    def setUp(self):
        msg = "Starting Test : %s : %s" % (self.id(), self.glustotest_run_id)
        g.log.info(msg)
        self.inject_msg_in_gluster_logs(msg)

    def tearDown(self):
        msg = "Ending Test: %s : %s" % (self.id(), self.glustotest_run_id)
        g.log.info(msg)
        self.inject_msg_in_gluster_logs(msg)

    @classmethod
    def tearDownClass(cls):
        msg = "Teardownclass: %s : %s" % (cls.__name__, cls.glustotest_run_id)
        g.log.info(msg)
        cls.inject_msg_in_gluster_logs(msg)


class GlusterBlockBaseClass(GlusterBaseClass):
    """GlusterBlockBaseClass sets up the volume and blocks.
    """
    @classmethod
    def setup_blocks(cls, blocknames):
        """Create blocks and calls the methods:
        update_block_info_dict and create_client_block_map

        Args:
            blocknames(list): Blocks to be create
        Returns:
            bool: False if block creation is unsuccessful and
            true if all blocks created.

        """
        if not isinstance(blocknames, list):
            blocknames = [blocknames]

        g.log.info("Creating block devices on volume %s", cls.volname)
        for blockname in blocknames:
            each_block = cls.gluster_block_args_info.get(blockname)
            if each_block:
                # Form a dict for keyargs
                block_args_info = {}
                block_args_info['ha'] = each_block['ha']
                block_args_info['auth'] = each_block['auth']
                block_args_info['prealloc'] = each_block['prealloc']
                block_args_info['storage'] = each_block['storage']
                block_args_info['ring-buffer'] = each_block['ring-buffer']

                _rc = setup_block(
                    mnode=cls.mnode,
                    volname=each_block['volname'],
                    blockname=each_block['blockname'],
                    servers=each_block['servers'],
                    size=each_block['size'],
                    **block_args_info)
                if not _rc:
                    g.log.error("Failed to create block on volume "
                                "%s: \n%s", cls.volname, each_block)
                    return False
                g.log.info("Successfully created block on volume "
                           "%s: \n%s", cls.volname, each_block)
            else:
                g.log.error("Unable to get args info for block %s on "
                            "volume %s", blockname, cls.volname)
                return False

        # Check if all the blocks are listed in block list command
        for blockname in blocknames:
            each_block = cls.gluster_block_args_info.get(blockname)
            _rc = if_block_exists(cls.mnode, each_block['volname'], blockname)
            if not _rc:
                return False

        # Update the block info dict
        cls.update_block_info_dict()
        # Create client-block map
        cls.create_client_block_map(cls.blocknames)
        return True

    @classmethod
    def update_block_info_dict(cls):
        """Updates the class's block_info_dict variable
        Calls the gluster-block info command and updates the block info.
        """
        # Get Block dict
        cls.blocknames = get_block_list(cls.mnode, cls.volname)

        if cls.blocknames:
            for blockname in cls.blocknames:
                cls.block_info_dict[blockname] = (get_block_info(cls.mnode,
                                                                 cls.volname,
                                                                 blockname))
                if cls.block_info_dict[blockname] is None:
                    g.log.error("Could not get block info")
                    return False
        # Update total_number_of_blocks
        cls.total_num_of_blocks = len(cls.blocknames)

        # Log the block_info_dict
        g.log.info("Logging Block Info:")
        for key, value in cls.block_info_dict.iteritems():
            g.log.info("Glusto block info: %s\n %s" % (key, value))

        return True

    @classmethod
    def discover_blocks_on_clients(cls, blocknames):
        """Discover blocks on all the clients
        """
        # List all the block devices on clients (Logging)

        if not isinstance(blocknames, list):
            blocknames = [blocknames]

        # results = g.run_parallel(cls.clients, "lsblk -S")
        server_list = []
        for blockname in blocknames:
            block_info = get_block_info(cls.mnode, cls.volname, blockname)
            if block_info:
                servers_to_add = block_info.get("EXPORTED ON")
                for server_ip in servers_to_add:
                    if server_ip not in server_list:
                        server_list.append(server_ip)
            else:
                g.log.error("Failed to get block info for block %s"
                            " on volume %s", blockname, cls.volname)
                return False

        g.log.info("Server list %s", server_list)
        # Discover the block devices from clients
        for client in cls.clients:
            for server in server_list:
                cmd = ("iscsiadm -m discovery -t st -p %s" %
                       server)
                ret, out, err = g.run(client, cmd)
                if ret != 0:
                    g.log.error("Failed to discover blocks on "
                                "client %s: %s", client, err)
                    return False
                g.log.info("Discovered blocks on client %s: %s",
                           client, out)
        return True

    @classmethod
    def get_iqn_of_blocks_on_clients(cls, blocknames):
        """Get iqn number of each block on it's respective client.

        Args:
            blocknames: list
        Returns:
            bool: True if iqn of all blocks is obtained. False otherwise
        """
        if not isinstance(blocknames, list):
            blocknames = [blocknames]
        for blockname in blocknames:

            try:
                block_gbid = cls.block_info_dict[blockname]['GBID']
            except KeyError:
                g.log.error("Failed to get GBID of block %s on volume %s",
                            blockname, cls.volname)
                return False

            try:
                block_mapped_client = (
                    cls.clients_blocks_map[blockname]['client'])
            except KeyError:
                g.log.error("Failed to get the client which mounts the block "
                            "%s on the volume %s", blockname, cls.volname)
                return False

            # Get the servers where the blocks are exported
            server_ip = cls.block_info_dict[blockname]['EXPORTED ON'][0]

            # Get iqn from gbid
            cmd = ("iscsiadm -m discovery -t st -p %s | grep -F %s | "
                   "tail -1 | cut -d ' ' -f2" %
                   (server_ip, block_gbid))

            # Not using async here as if two processes execute the above
            # command at the same time in background, it will cause:
            # 'iscsiadm: Connection to Discovery Address' error
            ret, out, err = g.run(block_mapped_client, cmd)
            if ret != 0:
                g.log.error("Failed to get iqn of block %s on client %s: %s",
                            block_gbid, block_mapped_client, err)
                return False
            g.log.info("Iqn for gbid '%s' on client %s : '%s'",
                       block_gbid, block_mapped_client, out)
            block_iqn = out.strip()
            cls.clients_blocks_map[blockname]['iqn'] = block_iqn

        return True

    @classmethod
    def login_to_iqn_on_clients(cls, blocknames):
        """Login the blocks on their clients/initiator.

        Return:
            Either bool or Execution error.
        """
        if not isinstance(blocknames, list):
            blocknames = [blocknames]

        ret = cls.update_block_info_dict()
        if not ret:
            return False

        result = cls.get_iqn_of_blocks_on_clients(blocknames)
        if not result:
            return False

        # ret_value = True
        # Login to the block from the client
        for blockname in blocknames:
            block_gbid = cls.block_info_dict[blockname]['GBID']
            block_mapped_client = (
                cls.clients_blocks_map[blockname]['client'])

            if not cls.clients_blocks_map[blockname].get('iqn'):
                g.log.error("Iqn info for block %s not there. So can't login",
                            blockname)
                return False

            block_iqn = cls.clients_blocks_map[blockname]['iqn']
            cls.clients_blocks_map[blockname]['logged_in'] = False

            if cls.block_info_dict[blockname]['PASSWORD']:
                block_password = cls.block_info_dict[blockname]['PASSWORD']
                cmd = ("iscsiadm -m node -T %s -o update -n "
                       "node.session.auth.authmethod -v CHAP -n "
                       "node.session.auth.username -v %s -n "
                       "node.session.auth.password -v %s " %
                       (block_iqn, block_gbid, block_password))
                ret, out, err = g.run(block_mapped_client, cmd)
                if ret != 0:
                    g.log.error("Unable to update login credentials for "
                                "iqn %s on %s: %s",
                                block_iqn, block_mapped_client, err)
                    return False
                g.log.info("Credentials for iqn %s updated successfully "
                           "on %s",
                           block_iqn, block_mapped_client)

            # Login to iqn
            if not cls.clients_blocks_map[blockname].get('logged_in'):
                cmd = "iscsiadm -m node -T %s -l" % block_iqn
                ret, out, err = g.run(block_mapped_client, cmd)
                if ret != 0:
                    raise ExecutionError("Failed to login to iqn %s on "
                                         "%s: %s Command o/p: %s ",
                                         block_iqn, block_mapped_client,
                                         err, out)

                g.log.info("Successfully logged in to iqn %s on %s: %s",
                           block_iqn, block_mapped_client, out)
                cls.clients_blocks_map[blockname]['logged_in'] = True

        return True

    @classmethod
    def logout_iqn_on_clients(cls, blocknames):
        """Logout each block from the initiator
        """
        # Convert string or unicode type to list
        if not isinstance(blocknames, list):
            blocknames = [blocknames]

        for blockname in blocknames:
            block_mapped_client = (
                cls.clients_blocks_map[blockname]['client'])
            block_iqn = cls.clients_blocks_map[blockname]['iqn']
            cmd = "iscsiadm -m node -T %s -u" % block_iqn
            ret, out, err = g.run(block_mapped_client, cmd)
            if ret != 0:
                g.log.error("Failed to logout of iqn %s on %s: %s"
                            " Command o/p: %s",
                            block_iqn, block_mapped_client, err, out)
                return False
            g.log.info("Successfully logged out of iqn %s on %s: %s",
                       block_iqn, block_mapped_client, out)

        return True

    @classmethod
    def get_mpath_of_iqn_on_clients(cls, blocknames):
        """Get mpath of the logged in blocks

        Return:
            True if successful and execution error if getting mpath fails.
        """
        # Get the mpath for iqn
        # Donot forget to install 'sg3_utils'
        # Convert string or unicode type to list
        if not isinstance(blocknames, list):
            blocknames = [blocknames]

        for blockname in blocknames:
            block_gbid = cls.block_info_dict[blockname]['GBID']
            block_mapped_client = cls.clients_blocks_map[blockname]['client']
            block_iqn = cls.clients_blocks_map[blockname]['iqn']
            if not cls.clients_blocks_map[blockname].get('mpath'):
                cmd = ("for i in `/dev/mapper/mpath*` ; do "
                       "sg_inq -i $i | grep %s > /dev/null ; "
                       "if [[ $? -eq 0 ]] ; then  echo $i ; fi ; done" %
                       (block_gbid))
                ret, out, err = g.run(block_mapped_client, cmd)
                if ret != 0:
                    raise ExecutionError("Failed to get mpath for iqn %s on "
                                         "client %s: %s", block_iqn,
                                         block_mapped_client, err)
                block_mpath = out.strip()
                g.log.info("Successfully got mpath '%s' for iqn '%s' on "
                           "client %s", block_mpath, block_iqn,
                           block_mapped_client)
                cls.clients_blocks_map[blockname]['mpath'] = block_mpath
                time.sleep(1)

        return True

    @classmethod
    def create_client_block_map(cls, blocknames):
        """
        Mapping a single block to a client.
        Select a client randomly from the list
        """
        if not isinstance(blocknames, list):
            blocknames = [blocknames]

        tmp_client_list = cls.clients[:]
        for blockname in blocknames:
            if blockname not in cls.clients_blocks_map:
                if len(tmp_client_list) == 0:
                    tmp_client_list = cls.clients[:]
                client_to_map = random.choice(tmp_client_list)
                tmp_client_list.remove(client_to_map)
                cls.clients_blocks_map[blockname] = {
                    'client': client_to_map,
                    'iqn': '',
                    'logged_in': False,
                    'mpath': '',
                    'is_formatted': False,
                    'is_mounted': False,
                    }
        g.log.info("Blocks mapped to clients. Each block is mapped to a  "
                   "randomly selected client")
        for blockname in blocknames:
            g.log.info("Block %s mapped to %s", blockname,
                       cls.clients_blocks_map[blockname]['client'])

    @classmethod
    def mount_blocks(cls, blocknames, filesystem='xfs'):
        """Mount the blocks on their clients
        """
        if not isinstance(blocknames, list):
            blocknames = [blocknames]
        # Discover the block on client
        ret = cls.discover_blocks_on_clients(blocknames)
        if not ret:
            return False

        for blockname in blocknames:
            if not cls.clients_blocks_map[blockname]['logged_in']:

                # Login inside the block on client
                ret = cls.login_to_iqn_on_clients(blockname)
                if not ret:
                    return False

                # time.sleep added because the path /dev/mapper/mapth*
                # is getting read even before the logging is completed.
                time.sleep(2)
                # Get mpath of block on it's client
                ret = cls.get_mpath_of_iqn_on_clients(blockname)
                if not ret:
                    return False

            # make fs
            block_mpath = cls.clients_blocks_map[blockname]['mpath']
            block_mapped_client = cls.clients_blocks_map[blockname]['client']
            if not cls.clients_blocks_map[blockname].get('is_formatted'):
                cmd = "mkfs.%s -f %s" % (filesystem, block_mpath)
                ret, out, err = g.run(block_mapped_client, cmd)
                if ret != 0:
                    raise ExecutionError("Failed to make fs on %s on client "
                                         "%s: %s", block_mpath,
                                         block_mapped_client, err)
                g.log.info("Successfully created fs on %s on client %s: %s",
                           block_mpath, block_mapped_client, out)
                cls.clients_blocks_map[blockname]['is_formatted'] = True

            # mount the block
            if not cls.clients_blocks_map[blockname].get('is_mounted'):
                temp_mount = {
                    'protocol': 'xfs',
                    'client': {
                        'host': cls.clients_blocks_map[blockname]['client'],
                            },
                    'volname': cls.clients_blocks_map[blockname]['mpath'],
                    'mountpoint': "/mnt/%s" % blockname
                    }
                mount_obj = create_mount_objs([temp_mount]).pop()

                g.log.info("Mount Obj %s", mount_obj)
                g.log.info("Mounting the device %s on %s:%s" %
                           (mount_obj.volname, mount_obj.client_system,
                            mount_obj.mountpoint))

                # The function is_mounted will give an error in log file:
                # "Missing arguments for mount"
                # Because this is also used for mounting glusterfs volumes and
                # a server name is needed But here mounting does not
                # require a server name and therefore the argument check
                # for server fails and an error is reported in the log file.
                # But that will not affect the block mounting.
                # So, we can live with it for now.
                ret = mount_obj.mount()
                if not ret:
                    raise ExecutionError("Unable to mount the "
                                         "device %s on %s:%s" %
                                         (mount_obj.volname,
                                          mount_obj.client_system,
                                          mount_obj.mountpoint))
                g.log.info("Successfully mounted the device %s on %s:%s" %
                           (mount_obj.volname, mount_obj.client_system,
                            mount_obj.mountpoint))
                cls.mount_blocks_list.append(mount_obj)
                cls.clients_blocks_map[blockname]['is_mounted'] = True

        return True

    @classmethod
    def setup_block_mount_block(cls, blocknames):
        """Create and mount the blocks
        """
        # Setup block
        g.log.info("Setting up blocks")
        ret = cls.setup_blocks(blocknames)
        if not ret:
            raise ExecutionError("Failed to setup blocks")
        g.log.info("Successful in setting up blocks")

        # Mount Blocks
        g.log.info("Mounting the blocks on initiator nodes")
        ret = cls.mount_blocks(blocknames)
        if not ret:
            raise ExecutionError("Failed to mount the blocks of volume %s",
                                 cls.volname)
        g.log.info("Successful in mounting the blocks of the volume %s",
                   cls.volname)

        return True

    @classmethod
    def get_block_args_info_from_config(cls):
        """Created the dict gluster_block_args_info which helps in
        providing block information during block creation
        """
        # Get gluster block info from config file
        if g.config.get('gluster_block_args_info'):
            cls.gluster_block_args_info = {}
            blocks_count = 0
            each_block_info = g.config['gluster_block_args_info']
            # for i, each_block_info in enumerate(
            # g.config['gluster_block_args_info']):
            # volname
            block_on_volume = cls.volname
            if each_block_info.get('volname'):
                block_on_volume = each_block_info['volname']

            # Block name
            block_base_name = "gluster_block"
            if each_block_info.get('blockname'):
                block_base_name = each_block_info['blockname']

            # servers
            block_servers = cls.servers
            if each_block_info.get('servers'):
                block_servers = each_block_info['servers']
                if not filter(None, block_servers):
                    block_servers = cls.servers

            # Block size
            block_size = "1GiB"
            if each_block_info.get('size'):
                block_size = each_block_info['size']

            # HA
            block_ha = 3
            if each_block_info.get('ha'):
                block_ha = each_block_info['ha']

            # auth
            block_auth = None
            if each_block_info.get('auth'):
                block_auth = each_block_info['auth']

            # prealloc
            block_prealloc = None
            if each_block_info.get('prealloc'):
                block_prealloc = each_block_info['prealloc']

            # ring-buffer
            block_ring_buffer = None
            if each_block_info.get('ring-buffer'):
                block_ring_buffer = each_block_info['ring-buffer']

            # Number of blocks
            num_of_blocks = 1
            if each_block_info.get('num_of_blocks'):
                num_of_blocks = int(each_block_info['num_of_blocks'])

            # for count in range(blocks_count,num_of_blocks +blocks_count):
            for count in range(blocks_count, num_of_blocks):
                # blocks_count = int(count) + i

                if block_ha:
                    selected_block_servers = random.sample(block_servers,
                                                           block_ha)
                else:
                    selected_block_servers = random.choice(block_servers)

                block_name = "_".join([block_base_name,
                                       str(count + 1)])

                cls.gluster_block_args_info[block_name] = (
                    {'volname': block_on_volume,
                     'blockname': block_name,
                     'servers': cls.get_ip_from_hostname(
                         selected_block_servers),
                     'size': block_size,
                     'ha': block_ha,
                     'auth': block_auth,
                     'prealloc': block_prealloc,
                     'storage': None,
                     'ring-buffer': block_ring_buffer}
                    )

        for key in cls.gluster_block_args_info.keys():
            value = cls.gluster_block_args_info[key]
            g.log.info("Gluster-Block args info: %s\n %s" % (key, value))

    @classmethod
    def setUpClass(cls, setup_vol=True, setup_blk=True, mount_blk=True):
        """Setup volume, create blocks, mount the blocks if specified.
        """
        GlusterBaseClass.setUpClass.im_func(cls)

        cls.mount_blocks_list = []
        cls.total_num_of_blocks = 0
        cls.block_info_dict = {}
        cls.clients_blocks_map = {}
        cls.blocknames = []

        # Default gluster block info
        cls.gluster_block_args_info = {
            'gluster_block_%d' % (cls.total_num_of_blocks + 1): {
                'volname': cls.volname,
                'blockname': 'gluster_block_%d'
                             % (cls.total_num_of_blocks + 1),
                'servers': random.sample(cls.servers_ips, 2),
                'size': '1GiB',
                'ha': 2,
                'auth': None,
                'prealloc': None,
                'storage': None,
                'ring-buffer': None
                }
            }

        if g.config.get('gluster_block_args_info'):
            cls.get_block_args_info_from_config()

    @classmethod
    def tearDownClass(cls, umount_blocks=True, cleanup_blocks=True,
                      cleanup_vol=True, unlink_storage="yes"):
        """Teardown the mounts, deletes blocks, gluster volume.
        """
        # Unmount volume
        if umount_blocks:
            _rc = True
            g.log.info("Starting to UnMount Blocks")
            for mount_obj in cls.mount_blocks_list:
                ret = mount_obj.unmount()
                if not ret:
                    g.log.error("Unable to unmount block '%s on cleint %s "
                                "at %s'",
                                mount_obj.volname, mount_obj.client_system,
                                mount_obj.mountpoint)
                    _rc = False
            if not _rc:
                raise ExecutionError("Unmount of all mounts are not "
                                     "successful")
            else:
                g.log.info("Successful in unmounting volume on all clients")
        else:
            g.log.info("Not Unmounting the Volume as 'umount_vol' is set "
                       "to %s", umount_blocks)

        # Logout the blocks
        for blockname in cls.clients_blocks_map:
            block_iqn = cls.clients_blocks_map[blockname]['iqn']
            block_mapped_client = (
                cls.clients_blocks_map[blockname]['client'])
            g.log.info("Logging out iqn %s on client %s", block_iqn,
                       block_mapped_client)
            cmd = "iscsiadm -m node -T %s -u" % block_iqn
            ret, out, err = g.run(block_mapped_client, cmd)
            if ret != 0:
                raise ExecutionError("Failed to logout iqn %s on client %s "
                                     ":%s", block_iqn, block_mapped_client,
                                     err)
            g.log.info("Successfully logged out iqn %s on client %s: %s",
                       block_iqn, block_mapped_client, out)

        # Restarting multipathd on all clients
        g.log.info("Restarting multipathd on all clients")
        cmd = "service multipathd restart && service multipathd status"
        results = g.run_parallel(cls.clients, cmd)
        for client in results:
            ret, out, err = results[client]
            if ret != 0:
                raise ExecutionError("Failed to restart multipathd on "
                                     "client %s: %s", client, err)
            g.log.info("Successfully restarted multipathd on client %s: %s",
                       client, out)

        # Cleanup blocks
        if cleanup_blocks:
            blocknames = get_block_list(cls.mnode, cls.volname)
            if blocknames:
                g.log.info("Listing blocks before deleting:\n%s",
                           '\n'.join(blocknames))
                for blockname in blocknames:
                    ret, out, err = block_delete(cls.mnode, cls.volname,
                                                 blockname, unlink_storage)
                    if ret != 0:
                        raise ExecutionError("Failed to delete the block "
                                             "%s on volume %s", blockname,
                                             cls.volname)
                    g.log.info("Successfully deleted the block %s on "
                               "volume %s", blockname, cls.volname)

        # Cleanup volume
        if cleanup_vol:
            g.log.info("Cleanup Volume %s", cls.volname)
            ret = cleanup_volume(mnode=cls.mnode, volname=cls.volname)
            if not ret:
                raise ExecutionError("cleanup volume %s failed", cls.volname)
            else:
                g.log.info("Successfully cleaned-up volume")
        else:
            g.log.info("Not Cleaning-Up volume as 'cleanup_vol' is %s",
                       cleanup_vol)

        GlusterBaseClass.tearDownClass.im_func(cls)
