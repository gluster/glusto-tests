#  Copyright (C) 2018-2020 Red Hat, Inc. <http://www.redhat.com>
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

from copy import deepcopy
from datetime import datetime
from inspect import isclass
from os.path import join as path_join
from random import choice as random_choice
from socket import (
    gethostbyname,
    gaierror,
)
from unittest import TestCase
from time import sleep

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import (
    ConfigError,
    ExecutionError,
)
from glustolibs.gluster.lib_utils import inject_msg_in_logs
from glustolibs.gluster.mount_ops import create_mount_objs
from glustolibs.gluster.nfs_libs import export_volume_through_nfs
from glustolibs.gluster.peer_ops import (
    is_peer_connected,
    peer_probe_servers, peer_status
)
from glustolibs.gluster.gluster_init import (
    restart_glusterd, stop_glusterd, wait_for_glusterd_to_start)
from glustolibs.gluster.samba_libs import share_volume_over_smb
from glustolibs.gluster.shared_storage_ops import is_shared_volume_mounted
from glustolibs.gluster.volume_libs import (
    cleanup_volume,
    log_volume_info_and_status,
    setup_volume,
    wait_for_volume_process_to_be_online,
)
from glustolibs.gluster.brick_libs import (
    wait_for_bricks_to_be_online, get_offline_bricks_list)
from glustolibs.gluster.volume_ops import (
    set_volume_options, volume_reset, volume_start)
from glustolibs.io.utils import log_mounts_info
from glustolibs.gluster.geo_rep_libs import setup_master_and_slave_volumes
from glustolibs.gluster.nfs_ganesha_ops import (
    teardown_nfs_ganesha_cluster)
from glustolibs.misc.misc_libs import kill_process


class runs_on(g.CarteTestClass):
    """Decorator providing runs_on capability for standard unittest script."""

    def __init__(self, value):
        # the names of the class attributes set by the runs_on decorator
        self.axis_names = ['volume_type', 'mount_type']

        # the options to replace 'ALL' in selections
        self.available_options = [['distributed', 'replicated',
                                   'distributed-replicated',
                                   'dispersed', 'distributed-dispersed',
                                   'arbiter', 'distributed-arbiter'],
                                  ['glusterfs', 'nfs', 'cifs', 'smb']]

        # these are the volume and mount options to run and set in config
        # what do runs_on_volumes and runs_on_mounts need to be named????
        run_on_volumes, run_on_mounts = self.available_options[0:2]
        if g.config.get('gluster', {}).get('running_on_volumes'):
            run_on_volumes = g.config['gluster']['running_on_volumes']

        if g.config.get('gluster', {}).get('running_on_mounts'):
            run_on_mounts = g.config['gluster']['running_on_mounts']

        # selections is the above info from the run that is intersected with
        # the limits from the test script
        self.selections = [run_on_volumes, run_on_mounts]

        # value is the limits that are passed in by the decorator
        self.limits = value


class GlusterBaseClass(TestCase):
    """GlusterBaseClass to be subclassed by Gluster Tests.
    This class reads the config for variable values that will be used in
    gluster tests. If variable values are not specified in the config file,
    the variable are defaulted to specific values.
    """
    # these will be populated by either the runs_on decorator or
    # defaults in setUpClass()
    volume_type = None
    mount_type = None
    error_or_failure_exists = False

    @staticmethod
    def get_super_method(obj, method_name):
        """PY2/3 compatible method for getting proper parent's (super) methods.

        Useful for test classes wrapped by 'runs_on' decorator which has
        duplicated original test class [py3] as parent instead of the
        base class as it is expected.

        Example for calling 'setUp()' method of the base class from the
        'setUp' method of a test class which was decorated with 'runs_on':

        @runs_on([['distributed'], ['glusterfs']])
        class TestDecoratedClass(GlusterBaseClass):
            ...
            @classmethod
            def setUpClass(cls):
                cls.get_super_method(cls, 'setUpClass')()
            ...
            def setUp(self):
                self.get_super_method(self, 'setUp')()
            ...

        """
        current_type = obj if isclass(obj) else obj.__class__
        while getattr(super(current_type, obj), method_name) == getattr(
                obj, method_name):
            current_type = current_type.__base__
        return getattr(super(current_type, obj), method_name)

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
        if not isinstance(nodes, list):
            nodes = [nodes]
        for node in nodes:
            try:
                ip = gethostbyname(node)
            except gaierror as e:
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

    def _is_error_or_failure_exists(self):
        """Function to get execution error in case of
        failures in testcases
        """
        if hasattr(self, '_outcome'):
            # Python 3.4+
            result = self.defaultTestResult()
            self._feedErrorsToResult(result, self._outcome.errors)
        else:
            # Python 2.7-3.3
            result = getattr(
                self, '_outcomeForDoCleanups', self._resultForDoCleanups)
        ok_result = True
        for attr in ('errors', 'failures'):
            if not hasattr(result, attr):
                continue
            exc_list = getattr(result, attr)
            if exc_list and exc_list[-1][0] is self:
                ok_result = ok_result and not exc_list[-1][1]
        if hasattr(result, '_excinfo'):
            ok_result = ok_result and not result._excinfo
        if ok_result:
            return False
        self.error_or_failure_exists = True
        GlusterBaseClass.error_or_failure_exists = True
        return True

    @classmethod
    def scratch_cleanup(cls, error_or_failure_exists):
        """
        This scratch_cleanup script will run only when the code
        currently running goes into execution or assertion error.

        Args:
            error_or_failure_exists (bool): If set True will cleanup setup
                atlast of testcase only if exectution or assertion error in
                teststeps. False will skip this scratch cleanup step.

        Returns (bool): True if setup cleanup is successful.
            False otherwise.
        """
        if error_or_failure_exists:
            shared_storage_mounted = False
            if is_shared_volume_mounted(cls.mnode):
                shared_storage_mounted = True
            ret = stop_glusterd(cls.servers)
            if not ret:
                g.log.error("Failed to stop glusterd")
                cmd_list = ("pkill `pidof glusterd`",
                            "rm /var/run/glusterd.socket")
                for server in cls.servers:
                    for cmd in cmd_list:
                        ret, _, _ = g.run(server, cmd, "root")
                        if ret:
                            g.log.error("Failed to stop glusterd")
                            return False
            for server in cls.servers:
                ret, out, _ = g.run(server, "pgrep glusterfsd", "root")
                if not ret:
                    ret = kill_process(server,
                                       process_ids=out.strip().split('\n'))
                    if not ret:
                        g.log.error("Unable to kill process {}".format(
                            out.strip().split('\n')))
                        return False
                if not shared_storage_mounted:
                    cmd_list = (
                        "rm -rf /var/lib/glusterd/vols/*",
                        "rm -rf /var/lib/glusterd/snaps/*",
                        "rm -rf /var/lib/glusterd/peers/*",
                        "rm -rf {}/*/*".format(
                            cls.all_servers_info[server]['brick_root']))
                else:
                    cmd_list = (
                        "for vol in `ls /var/lib/glusterd/vols/ | "
                        "grep -v gluster_shared_storage`;do "
                        "rm -rf /var/lib/glusterd/vols/$vol;done",
                        "rm -rf /var/lib/glusterd/snaps/*"
                        "rm -rf {}/*/*".format(
                            cls.all_servers_info[server]['brick_root']))
                for cmd in cmd_list:
                    ret, _, _ = g.run(server, cmd, "root")
                    if ret:
                        g.log.error(
                            "failed to cleanup server {}".format(server))
                        return False
            ret = restart_glusterd(cls.servers)
            if not ret:
                g.log.error("Failed to start glusterd")
                return False
            sleep(2)
            ret = wait_for_glusterd_to_start(cls.servers)
            if not ret:
                g.log.error("Failed to bring glusterd up")
                return False
            if not shared_storage_mounted:
                ret = peer_probe_servers(cls.mnode, cls.servers)
                if not ret:
                    g.log.error("Failed to peer probe servers")
                    return False
            for client in cls.clients:
                cmd_list = ("umount /mnt/*", "rm -rf /mnt/*")
                for cmd in cmd_list:
                    ret = g.run(client, cmd, "root")
                    if ret:
                        g.log.error(
                            "failed to unmount/already unmounted {}"
                            .format(client))
            return True

    @classmethod
    def setup_volume(cls, volume_create_force=False, only_volume_create=False):
        """Setup the volume:
            - Create the volume, Start volume, Set volume
            options, enable snapshot/quota if specified in the config
            file.
            - Wait for volume processes to be online
            - Export volume as NFS/SMB share if mount_type is NFS or SMB
            - Log volume info and status

        Args:
            volume_create_force(bool): True if create_volume should be
                executed with 'force' option.
            only_volume_create(bool): True, only volume creation is needed
                                      False, by default volume creation and
                                      start.

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
                           volume_config=cls.volume, force=force_volume_create,
                           create_only=only_volume_create)
        if not ret:
            g.log.error("Failed to Setup volume %s", cls.volname)
            return False
        g.log.info("Successful in setting up volume %s", cls.volname)

        # Returning the value without proceeding for next steps
        if only_volume_create and ret:
            g.log.info("Setup volume with volume creation {} "
                       "successful".format(cls.volname))
            return True

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
    def bricks_online_and_volume_reset(cls):
        """
        reset the volume if any bricks are offline.
        waits for all bricks to be online and resets
        volume options set
        """
        bricks_offline = get_offline_bricks_list(cls.mnode, cls.volname)
        if bricks_offline is not None:
            ret = volume_start(cls.mnode, cls.volname, force=True)
            if not ret:
                raise ExecutionError("Failed to force start volume"
                                     "%s" % cls.volname)
        ret = wait_for_bricks_to_be_online(cls.mnode, cls.volname)
        if not ret:
            raise ExecutionError("Failed to bring bricks online"
                                 "for volume %s" % cls.volname)

        ret, _, _ = volume_reset(cls.mnode, cls.volname, force=True)
        if ret:
            raise ExecutionError("Failed to reset volume %s" % cls.volname)
        g.log.info("Successful in volume reset %s", cls.volname)

    @classmethod
    def setup_and_mount_geo_rep_master_and_slave_volumes(cls, force=False):
        """Setup geo-rep master and slave volumes.

        Returns (bool): True if cleanup volume is successful. False otherwise.
        """
        # Creating and starting master and slave volume.
        ret = setup_master_and_slave_volumes(
            cls.mode, cls.all_servers_info, cls.master_volume,
            cls.snode, cls.all_slaves_info, cls.slave_volume,
            force)
        if not ret:
            g.log.error('Failed to create master and slave volumes.')
            return False

        # Mounting master and slave volumes
        for mount in [cls.master_mounts, cls.slave_mounts]:
            ret = cls.mount_volume(cls, mount)
            if not ret:
                g.log.error('Failed to mount volume %s.',
                            mount['volname'])
                return False
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

            g.log.info("Starting to delete the directory path used for "
                       "mounting")
            cmd = ('rm -rf %s' % mount_obj.mountpoint)
            ret, _, err = g.run(
                mount_obj.client_system, cmd, user=mount_obj.user)
            if ret:
                g.log.error(
                    "failed to delete the directory path used for "
                    "mounting %s: %s" % (mount_obj.mountpoint, err))
                return False

            g.log.info(
                "Successful in deleting the directory path used for "
                "mounting '%s:%s' on '%s:%s'" % (
                    mount_obj.server_system,
                    mount_obj.volname, mount_obj.client_system,
                    mount_obj.mountpoint))

        # Get mounts info
        g.log.info("Get mounts Info:")
        log_mounts_info(mounts)

        return True

    @classmethod
    def get_unique_lv_list_from_all_servers(cls):
        """Get all unique lv path from all servers

        Returns: List of all unique lv path in all servers. None otherwise.
        """
        cmd = "lvs --noheadings -o lv_path | awk '{if ($1) print $1}'"
        lv_list = []
        for server in cls.servers:
            ret, out, _ = g.run(server, cmd, "root")
            current_lv_list = out.splitlines()
            if current_lv_list:
                lv_list.extend(current_lv_list)
            if ret:
                g.log.error("failed to execute command %s" % cmd)
                raise ExecutionError("Failed to execute %s cmd" % cmd)
        return list(set(lv_list))

    @classmethod
    def cleanup_volume(cls):
        """Cleanup the volume

        Returns (bool): True if cleanup volume is successful. False otherwise.
        """
        cls.bricks_online_and_volume_reset()
        g.log.info("Cleanup Volume %s", cls.volname)
        ret = cleanup_volume(mnode=cls.mnode, volname=cls.volname)
        if not ret:
            g.log.error("cleanup of volume %s failed", cls.volname)
        else:
            g.log.info("Successfully cleaned-up volume %s", cls.volname)

        # Log Volume Info and Status
        g.log.info("Log Volume %s Info and Status", cls.volname)
        log_volume_info_and_status(cls.mnode, cls.volname)

        # compare and remove additional lv created, skip otherwise
        new_lv_list = cls.get_unique_lv_list_from_all_servers()
        if cls.lv_list != new_lv_list:
            cmd = ("for mnt in `mount | grep 'run/gluster/snaps' |"
                   "awk '{print $3}'`; do umount $mnt; done")
            for server in cls.servers:
                ret, _, err = g.run(server, cmd, "root")
                if ret:
                    g.log.error("Failed to remove snap "
                                "bricks from mountpoint %s" % err)
                    return False
            new_lv_list = cls.get_unique_lv_list_from_all_servers()
            lv_remove_list = list(set(new_lv_list) - set(cls.lv_list))
            for server in cls.servers:
                for lv in lv_remove_list:
                    cmd = ("lvremove %s --force" % lv)
                    ret, _, err = g.run(server, cmd, "root")
                    if ret:
                        g.log.error("failed to remove lv: %s" % err)
                    g.log.info("Expected error msg '%s'" % err)
        g.log.info("Successfully cleaned-up volumes")
        return True

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
        """Initialize all the variables necessary for testing Gluster."""
        # Get all servers
        cls.all_servers = None
        if 'servers' in g.config and g.config['servers']:
            cls.all_servers = g.config['servers']
            cls.servers = cls.all_servers
        else:
            raise ConfigError("'servers' not defined in the global config")

        # Get all slaves
        cls.slaves = None
        if g.config.get('slaves'):
            cls.slaves = g.config['slaves']
            # Set mnode_slave : Node on which slave commands are executed
            cls.mnode_slave = cls.slaves[0]
            # Slave IP's
            cls.slaves_ip = cls.get_ip_from_hostname(cls.slaves)

        # Get all clients
        cls.all_clients = None
        if g.config.get('clients'):
            cls.all_clients = g.config['clients']
            cls.clients = cls.all_clients
        else:
            raise ConfigError("'clients' not defined in the global config")

        # Get all servers info
        cls.all_servers_info = None
        if g.config.get('servers_info'):
            cls.all_servers_info = g.config['servers_info']
        else:
            raise ConfigError("'servers_info' not defined in the global "
                              "config")
        # Get all slaves info
        cls.all_slaves_info = None
        if g.config.get('slaves_info'):
            cls.all_slaves_info = g.config['slaves_info']

        # All clients_info
        cls.all_clients_info = None
        if g.config.get('clients_info'):
            cls.all_clients_info = g.config['clients_info']
        else:
            raise ConfigError("'clients_info' not defined in the global "
                              "config")

        # get lv list
        cls.lv_list = cls.get_unique_lv_list_from_all_servers()

        # Set mnode : Node on which gluster commands are executed
        cls.mnode = cls.all_servers[0]

        # Server IP's
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
            cls.enable_nfs_ganesha = (
                g.config['gluster']['cluster_config']['nfs_ganesha']['enable']
                in ('TRUE', 'True', 'true', 'YES', 'Yes', 'yes', '1', 1)
            )
            cls.num_of_nfs_ganesha_nodes = g.config['gluster'][
                'cluster_config']['nfs_ganesha']['num_of_nfs_ganesha_nodes']
            cls.vips = (
                g.config['gluster']['cluster_config']['nfs_ganesha']['vips'])
        except KeyError:
            cls.enable_nfs_ganesha = False
            cls.num_of_nfs_ganesha_nodes = None
            cls.vips = []

        # Geo-rep Cluster information
        try:
            cls.geo_rep_info = (g.config['gluster']['geo_rep']
                                ['cluster_config'])
        except KeyError:
            cls.geo_rep_info = {}
            cls.geo_rep_info['root'] = {}
            cls.geo_rep_info['user'] = {}
            cls.geo_rep_info['root']['password'] = ''
            cls.geo_rep_info['user']['name'] = ''
            cls.geo_rep_info['user']['password'] = ''
            cls.geo_rep_info['user']['group'] = ''

        # Defining default volume_types configuration.
        cls.default_volume_type_config = {
            'replicated': {
                'type': 'replicated',
                'replica_count': 3,
                'transport': 'tcp',
            },
            'dispersed': {
                'type': 'dispersed',
                'disperse_count': 6,
                'redundancy_count': 2,
                'transport': 'tcp',
            },
            'distributed': {
                'type': 'distributed',
                'dist_count': 4,
                'transport': 'tcp',
            },
            'distributed-replicated': {
                'type': 'distributed-replicated',
                'dist_count': 2,
                'replica_count': 3,
                'transport': 'tcp',
            },
            'distributed-dispersed': {
                'type': 'distributed-dispersed',
                'dist_count': 2,
                'disperse_count': 6,
                'redundancy_count': 2,
                'transport': 'tcp',
            },
            'arbiter': {
                'type': 'arbiter',
                'replica_count': 3,
                'arbiter_count': 1,
                'transport': 'tcp',
            },
            'distributed-arbiter': {
                'type': 'distributed-arbiter',
                'dist_count': 2,
                'replica_count': 3,
                'arbiter_count': 1,
                'tranport': 'tcp',
            }
        }

        # Check if default volume_type configuration is provided in config yml
        if g.config.get('gluster', {}).get('volume_types'):
            default_volume_type_from_config = (
                g.config['gluster']['volume_types'])
            for volume_type in default_volume_type_from_config.keys():
                if default_volume_type_from_config[volume_type]:
                    if volume_type in cls.default_volume_type_config:
                        cls.default_volume_type_config[volume_type] = (
                            default_volume_type_from_config[volume_type])

        # Create Volume with force option
        cls.volume_create_force = False
        if g.config.get('gluster', {}).get('volume_create_force'):
            cls.volume_create_force = (
                g.config['gluster']['volume_create_force'])

        # Default volume options which is applicable for all the volumes
        cls.volume_options = {}
        if g.config.get('gluster', {}).get('volume_options'):
            cls.volume_options = g.config['gluster']['volume_options']

        # If the volume is exported as SMB Share, then set the following
        # volume options on the share.
        cls.smb_share_options = {}
        if g.config.get('gluster', {}).get('smb_share_options'):
            cls.smb_share_options = g.config['gluster']['smb_share_options']

        # If the volume is exported as NFS-Ganesha export,
        # then set the following volume options on the export.
        cls.nfs_ganesha_export_options = {}
        if g.config.get('gluster', {}).get('nfs_ganesha_export_options'):
            cls.nfs_ganesha_export_options = (
                g.config['gluster']['nfs_ganesha_export_options'])

        # Get the volume configuration.
        cls.volume = {}
        if cls.volume_type:
            for volume in g.config.get('gluster', {}).get('volumes', []):
                if volume['voltype']['type'] == cls.volume_type:
                    cls.volume = deepcopy(volume)
                    if 'name' not in cls.volume:
                        cls.volume['name'] = 'testvol_%s' % cls.volume_type
                    if 'servers' not in cls.volume:
                        cls.volume['servers'] = cls.all_servers
                    break
            else:
                try:
                    if g.config['gluster']['volume_types'][cls.volume_type]:
                        cls.volume['voltype'] = (g.config['gluster']
                                                 ['volume_types']
                                                 [cls.volume_type])
                except KeyError:
                    try:
                        cls.volume['voltype'] = (
                            cls.default_volume_type_config[cls.volume_type])
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

            # Define useful variable for geo-rep volumes.
            if cls.slaves:
                # For master volume
                cls.master_volume = cls.volume
                cls.master_volume['name'] = ('master_testvol_%s'
                                             % cls.volume_type)
                cls.master_volname = cls.master_volume['name']
                cls.master_voltype = (cls.master_volume['voltype']
                                      ['type'])

                # For slave volume
                cls.slave_volume = deepcopy(cls.volume)
                cls.slave_volume['name'] = ('slave_testvol_%s'
                                            % cls.volume_type)
                cls.slave_volume['servers'] = cls.slaves
                cls.slave_volname = cls.slave_volume['name']
                cls.slave_voltype = (cls.slave_volume['voltype']
                                     ['type'])

        # Get the mount configuration.
        cls.mounts = []
        if cls.mount_type:
            cls.mounts_dict_list = []
            for mount in g.config.get('gluster', {}).get('mounts', []):
                if mount['protocol'] != cls.mount_type:
                    continue
                temp_mount = {
                    'protocol': cls.mount_type,
                    'volname': cls.volname,
                }
                if mount.get('volname'):
                    if mount['volname'] == cls.volname:
                        temp_mount = deepcopy(mount)
                    else:
                        continue
                temp_mount.update({
                    'server': mount.get('server', cls.mnode),
                    'mountpoint': mount.get('mountpoint', path_join(
                        "/mnt", '_'.join([cls.volname, cls.mount_type]))),
                    'client': mount.get('client', cls.all_clients_info[
                        random_choice(list(cls.all_clients_info.keys()))]),
                    'options': mount.get('options', ''),
                })
                cls.mounts_dict_list.append(temp_mount)

            if not cls.mounts_dict_list:
                for client in cls.all_clients_info.keys():
                    cls.mounts_dict_list.append({
                        'protocol': cls.mount_type,
                        'server': cls.mnode,
                        'volname': cls.volname,
                        'client': cls.all_clients_info[client],
                        'mountpoint': path_join(
                            "/mnt", '_'.join([cls.volname, cls.mount_type])),
                        'options': '',
                    })

            if cls.mount_type == 'cifs' or cls.mount_type == 'smb':
                for mount in cls.mounts_dict_list:
                    if 'smbuser' not in mount:
                        mount['smbuser'] = random_choice(
                            list(cls.smb_users_info.keys()))
                        mount['smbpasswd'] = (
                            cls.smb_users_info[mount['smbuser']]['password'])

            cls.mounts = create_mount_objs(cls.mounts_dict_list)

            # Setting mounts for geo-rep volumes.
            if cls.slaves:

                # For master volume mount
                cls.master_mounts = cls.mounts

                # For slave volume mount
                slave_mount_dict_list = deepcopy(cls.mounts_dict_list)
                for mount_dict in slave_mount_dict_list:
                    mount_dict['volname'] = cls.slave_volume
                    mount_dict['server'] = cls.mnode_slave
                    mount_dict['mountpoint'] = path_join(
                        "/mnt", '_'.join([cls.slave_volname,
                                          cls.mount_type]))
                cls.slave_mounts = create_mount_objs(slave_mount_dict_list)

            # Defining clients from mounts.
            cls.clients = []
            for mount in cls.mounts_dict_list:
                cls.clients.append(mount['client']['host'])
            cls.clients = list(set(cls.clients))

        # Gluster Logs info
        cls.server_gluster_logs_dirs = ["/var/log/glusterfs", "/var/log/samba"]
        cls.server_gluster_logs_files = ["/var/log/ganesha.log",
                                         "/var/log/ganesha-gfapi.log"]
        if g.config.get('gluster', {}).get('server_gluster_logs_info'):
            server_gluster_logs_info = (
                g.config['gluster']['server_gluster_logs_info'])
            if server_gluster_logs_info.get('dirs'):
                cls.server_gluster_logs_dirs = server_gluster_logs_info['dirs']
            if server_gluster_logs_info.get('files'):
                cls.server_gluster_logs_files = (
                    server_gluster_logs_info['files'])

        cls.client_gluster_logs_dirs = ["/var/log/glusterfs"]
        cls.client_gluster_logs_files = []
        if g.config.get('gluster', {}).get('client_gluster_logs_info'):
            client_gluster_logs_info = (
                g.config['gluster']['client_gluster_logs_info'])
            if client_gluster_logs_info.get('dirs'):
                cls.client_gluster_logs_dirs = client_gluster_logs_info['dirs']
            if client_gluster_logs_info.get('files'):
                cls.client_gluster_logs_files = (
                    client_gluster_logs_info['files'])

        # Have a unique string to recognize the test run for logging in
        # gluster logs
        if 'glustotest_run_id' not in g.config:
            g.config['glustotest_run_id'] = (
                datetime.now().strftime('%H_%M_%d_%m_%Y'))
        cls.glustotest_run_id = g.config['glustotest_run_id']

        if cls.enable_nfs_ganesha:
            g.log.info("Setup NFS_Ganesha")
            cls.num_of_nfs_ganesha_nodes = int(cls.num_of_nfs_ganesha_nodes)
            cls.servers_in_nfs_ganesha_cluster = (
                cls.servers[:cls.num_of_nfs_ganesha_nodes])
            cls.vips_in_nfs_ganesha_cluster = (
                cls.vips[:cls.num_of_nfs_ganesha_nodes])

            # Obtain hostname of servers in ganesha cluster
            cls.ganesha_servers_hostname = []
            for ganesha_server in cls.servers_in_nfs_ganesha_cluster:
                ret, hostname, _ = g.run(ganesha_server, "hostname")
                if ret:
                    raise ExecutionError("Failed to obtain hostname of %s"
                                         % ganesha_server)
                hostname = hostname.strip()
                g.log.info("Obtained hostname: IP- %s, hostname- %s",
                           ganesha_server, hostname)
                cls.ganesha_servers_hostname.append(hostname)
            from glustolibs.gluster.nfs_ganesha_libs import setup_nfs_ganesha
            ret = setup_nfs_ganesha(cls)
            if not ret:
                raise ExecutionError("Failed to setup nfs ganesha")

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

    def doCleanups(self):
        if (self.error_or_failure_exists or
                self._is_error_or_failure_exists()):
            ret = self.scratch_cleanup(self.error_or_failure_exists)
            g.log.info(ret)
        return self.get_super_method(self, 'doCleanups')()

    @classmethod
    def doClassCleanups(cls):
        if (GlusterBaseClass.error_or_failure_exists or
                cls._is_error_or_failure_exists()):
            ret = cls.scratch_cleanup(
                GlusterBaseClass.error_or_failure_exists)
            g.log.info(ret)
        return cls.get_super_method(cls, 'doClassCleanups')()

    @classmethod
    def delete_nfs_ganesha_cluster(cls):
        ret = teardown_nfs_ganesha_cluster(
            cls.servers_in_nfs_ganesha_cluster)
        if not ret:
            g.log.error("Teardown got failed. Hence, cleaning up "
                        "nfs-ganesha cluster forcefully")
            ret = teardown_nfs_ganesha_cluster(
                cls.servers_in_nfs_ganesha_cluster, force=True)
            if not ret:
                raise ExecutionError("Force cleanup of nfs-ganesha "
                                     "cluster failed")
        g.log.info("Teardown nfs ganesha cluster succeeded")

    @classmethod
    def start_memory_and_cpu_usage_logging(cls, test_id, interval=60,
                                           count=100):
        """Upload logger script and start logging usage on cluster

        Args:
         test_id(str): ID of the test running fetched from self.id()

        Kawrgs:
         interval(int): Time interval after which logs are to be collected
                        (Default: 60)
         count(int): Number of samples to be collected(Default: 100)

        Returns:
         proc_dict(dict):Dictionary of logging processes
        """
        # imports are added inside function to make it them
        # optional and not cause breakage on installation
        # which don't use the resource leak library
        from glustolibs.io.memory_and_cpu_utils import (
            check_upload_memory_and_cpu_logger_script,
            log_memory_and_cpu_usage_on_cluster)

        # Checking if script is present on servers or not if not then
        # upload it to servers.
        if not check_upload_memory_and_cpu_logger_script(cls.servers):
            return None

        # Checking if script is present on clients or not if not then
        # upload it to clients.
        if not check_upload_memory_and_cpu_logger_script(cls.clients):
            return None

        # Start logging on servers and clients
        proc_dict = log_memory_and_cpu_usage_on_cluster(
            cls.servers, cls.clients, test_id, interval, count)

        return proc_dict

    @classmethod
    def compute_and_print_usage_stats(cls, test_id, proc_dict,
                                      kill_proc=False):
        """Compute and print CPU and memory usage statistics

        Args:
         proc_dict(dict):Dictionary of logging processes
         test_id(str): ID of the test running fetched from self.id()

        Kwargs:
         kill_proc(bool): Kill logging process if true else wait
                          for process to complete execution
        """
        # imports are added inside function to make it them
        # optional and not cause breakage on installation
        # which don't use the resource leak library
        from glustolibs.io.memory_and_cpu_utils import (
            wait_for_logging_processes_to_stop, kill_all_logging_processes,
            compute_data_usage_stats_on_servers,
            compute_data_usage_stats_on_clients)

        # Wait or kill running logging process
        if kill_proc:
            nodes = cls.servers + cls.clients
            ret = kill_all_logging_processes(proc_dict, nodes, cluster=True)
            if not ret:
                g.log.error("Unable to stop logging processes.")
        else:
            ret = wait_for_logging_processes_to_stop(proc_dict, cluster=True)
            if not ret:
                g.log.error("Processes didn't complete still running.")

        # Compute and print stats for servers
        ret = compute_data_usage_stats_on_servers(cls.servers, test_id)
        g.log.info('*' * 50)
        g.log.info(ret)  # TODO: Make logged message more structured
        g.log.info('*' * 50)

        # Compute and print stats for clients
        ret = compute_data_usage_stats_on_clients(cls.clients, test_id)
        g.log.info('*' * 50)
        g.log.info(ret)  # TODO: Make logged message more structured
        g.log.info('*' * 50)

    @classmethod
    def check_for_memory_leaks_and_oom_kills_on_servers(cls, test_id,
                                                        gain=30.0):
        """Check for memory leaks and OOM kills on servers

        Args:
         test_id(str): ID of the test running fetched from self.id()

        Kwargs:
         gain(float): Accepted amount of leak for a given testcase in MB
                      (Default:30)

        Returns:
         bool: True if memory leaks or OOM kills are observed else false
        """
        # imports are added inside function to make it them
        # optional and not cause breakage on installation
        # which don't use the resource leak library
        from glustolibs.io.memory_and_cpu_utils import (
            check_for_memory_leaks_in_glusterd,
            check_for_memory_leaks_in_glusterfs,
            check_for_memory_leaks_in_glusterfsd,
            check_for_oom_killers_on_servers)

        # Check for memory leaks on glusterd
        if check_for_memory_leaks_in_glusterd(cls.servers, test_id, gain):
            g.log.error("Memory leak on glusterd.")
            return True

        if cls.volume_type != "distributed":
            # Check for memory leaks on shd
            if check_for_memory_leaks_in_glusterfs(cls.servers, test_id,
                                                   gain):
                g.log.error("Memory leak on shd.")
                return True

        # Check for memory leaks on brick processes
        if check_for_memory_leaks_in_glusterfsd(cls.servers, test_id, gain):
            g.log.error("Memory leak on brick process.")
            return True

        # Check OOM kills on servers for all gluster server processes
        if check_for_oom_killers_on_servers(cls.servers):
            g.log.error('OOM kills present on servers.')
            return True
        return False

    @classmethod
    def check_for_memory_leaks_and_oom_kills_on_clients(cls, test_id, gain=30):
        """Check for memory leaks and OOM kills on clients

        Args:
         test_id(str): ID of the test running fetched from self.id()

        Kwargs:
         gain(float): Accepted amount of leak for a given testcase in MB
                      (Default:30)

        Returns:
         bool: True if memory leaks or OOM kills are observed else false
        """
        # imports are added inside function to make it them
        # optional and not cause breakage on installation
        # which don't use the resource leak library
        from glustolibs.io.memory_and_cpu_utils import (
            check_for_memory_leaks_in_glusterfs_fuse,
            check_for_oom_killers_on_clients)

        # Check for memory leak on glusterfs fuse process
        if check_for_memory_leaks_in_glusterfs_fuse(cls.clients, test_id,
                                                    gain):
            g.log.error("Memory leaks observed on FUSE clients.")
            return True

        # Check for oom kills on clients
        if check_for_oom_killers_on_clients(cls.clients):
            g.log.error("OOM kills present on clients.")
            return True
        return False

    @classmethod
    def check_for_cpu_usage_spikes_on_servers(cls, test_id, threshold=3):
        """Check for CPU usage spikes on servers

        Args:
         test_id(str): ID of the test running fetched from self.id()

        Kwargs:
         threshold(int): Accepted amount of instances of 100% CPU usage
                        (Default:3)
        Returns:
         bool: True if CPU spikes are more than threshold else False
        """
        # imports are added inside function to make it them
        # optional and not cause breakage on installation
        # which don't use the resource leak library
        from glustolibs.io.memory_and_cpu_utils import (
            check_for_cpu_usage_spikes_on_glusterd,
            check_for_cpu_usage_spikes_on_glusterfs,
            check_for_cpu_usage_spikes_on_glusterfsd)

        # Check for CPU usage spikes on glusterd
        if check_for_cpu_usage_spikes_on_glusterd(cls.servers, test_id,
                                                  threshold):
            g.log.error("CPU usage spikes observed more than threshold "
                        "on glusterd.")
            return True

        if cls.volume_type != "distributed":
            # Check for CPU usage spikes on shd
            if check_for_cpu_usage_spikes_on_glusterfs(cls.servers, test_id,
                                                       threshold):
                g.log.error("CPU usage spikes observed more than threshold "
                            "on shd.")
                return True

        # Check for CPU usage spikes on brick processes
        if check_for_cpu_usage_spikes_on_glusterfsd(cls.servers, test_id,
                                                    threshold):
            g.log.error("CPU usage spikes observed more than threshold "
                        "on shd.")
            return True
        return False

    @classmethod
    def check_for_cpu_spikes_on_clients(cls, test_id, threshold=3):
        """Check for CPU usage spikes on clients

        Args:
         test_id(str): ID of the test running fetched from self.id()

        Kwargs:
         threshold(int): Accepted amount of instances of 100% CPU usage
                        (Default:3)
        Returns:
         bool: True if CPU spikes are more than threshold else False
        """
        # imports are added inside function to make it them
        # optional and not cause breakage on installation
        # which don't use the resource leak library
        from glustolibs.io.memory_and_cpu_utils import (
            check_for_cpu_usage_spikes_on_glusterfs_fuse)

        ret = check_for_cpu_usage_spikes_on_glusterfs_fuse(cls.clients,
                                                           test_id,
                                                           threshold)
        return ret
