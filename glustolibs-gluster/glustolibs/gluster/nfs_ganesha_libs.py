#!/usr/bin/env python
#  Copyright (C) 2016-2017  Red Hat, Inc. <http://www.redhat.com>
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
    Description: nfs ganesha base classes.
    Pre-requisite: Please install gdeploy package on the glusto-tests
    management node.
"""

import time
import socket
import re
from glusto.core import Glusto as g
from glustolibs.gluster.nfs_ganesha_ops import (
    is_nfs_ganesha_cluster_exists,
    is_nfs_ganesha_cluster_in_healthy_state,
    teardown_nfs_ganesha_cluster,
    create_nfs_ganesha_cluster,
    export_nfs_ganesha_volume,
    unexport_nfs_ganesha_volume,
    configure_ports_on_clients,
    ganesha_client_firewall_settings)
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.exceptions import ExecutionError, ConfigError
from glustolibs.gluster.peer_ops import peer_probe_servers, peer_status
from glustolibs.gluster.volume_ops import volume_info, get_volume_info
from glustolibs.gluster.volume_libs import (setup_volume, cleanup_volume,
                                            log_volume_info_and_status,
                                            get_volume_options,
                                            is_volume_exported)
from glustolibs.gluster.mount_ops import create_mount_objs
from glustolibs.io.utils import log_mounts_info, wait_for_io_to_complete
from glustolibs.misc.misc_libs import upload_scripts


class NfsGaneshaClusterSetupClass(GlusterBaseClass):
    """Creates nfs ganesha cluster
    """
    @classmethod
    def setUpClass(cls):
        """
        Setup variable for nfs-ganesha tests.
        """
        # pylint: disable=too-many-statements, too-many-branches
        GlusterBaseClass.setUpClass.im_func(cls)

        # Check if enable_nfs_ganesha is set in config file
        if not cls.enable_nfs_ganesha:
            raise ConfigError("Please enable nfs ganesha in config")

        # Read num_of_nfs_ganesha_nodes from config file and create
        # nfs ganesha cluster accordingly
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

    @classmethod
    def setup_nfs_ganesha(cls):
        """
        Create nfs-ganesha cluster if not exists
        Set client configurations for nfs-ganesha

        Returns:
            True(bool): If setup is successful
            False(bool): If setup is failure
        """
        # pylint: disable = too-many-statements, too-many-branches
        # pylint: disable = too-many-return-statements
        cluster_exists = is_nfs_ganesha_cluster_exists(
            cls.servers_in_nfs_ganesha_cluster[0])
        if cluster_exists:
            is_healthy = is_nfs_ganesha_cluster_in_healthy_state(
                cls.servers_in_nfs_ganesha_cluster[0])

            if is_healthy:
                g.log.info("Nfs-ganesha Cluster exists and is in healthy "
                           "state. Skipping cluster creation...")
            else:
                g.log.info("Nfs-ganesha Cluster exists and is not in "
                           "healthy state.")
                g.log.info("Tearing down existing cluster which is not in "
                           "healthy state")
                ganesha_ha_file = ("/var/run/gluster/shared_storage/"
                                   "nfs-ganesha/ganesha-ha.conf")

                g.log.info("Collecting server details of existing "
                           "nfs ganesha cluster")
                conn = g.rpyc_get_connection(
                    cls.servers_in_nfs_ganesha_cluster[0], user="root")
                if not conn:
                    tmp_node = cls.servers_in_nfs_ganesha_cluster[0]
                    g.log.error("Unable to get connection to 'root' of node"
                                " %s", tmp_node)
                    return False

                if not conn.modules.os.path.exists(ganesha_ha_file):
                    g.log.error("Unable to locate %s", ganesha_ha_file)
                    return False
                with conn.builtin.open(ganesha_ha_file, "r") as fhand:
                    ganesha_ha_contents = fhand.read()
                g.rpyc_close_connection(
                    host=cls.servers_in_nfs_ganesha_cluster[0], user="root")
                servers_in_existing_cluster = re.findall(r'VIP_(.*)\=.*',
                                                         ganesha_ha_contents)

                ret = teardown_nfs_ganesha_cluster(
                    servers_in_existing_cluster, force=True)
                if not ret:
                    g.log.error("Failed to teardown unhealthy ganesha "
                                "cluster")
                    return False

                g.log.info("Existing unhealthy cluster got teardown "
                           "successfully")

        if (not cluster_exists) or (not is_healthy):
            g.log.info("Creating nfs-ganesha cluster of %s nodes"
                       % str(cls.num_of_nfs_ganesha_nodes))
            g.log.info("Nfs-ganesha cluster node info: %s"
                       % cls.servers_in_nfs_ganesha_cluster)
            g.log.info("Nfs-ganesha cluster vip info: %s"
                       % cls.vips_in_nfs_ganesha_cluster)

            ret = create_nfs_ganesha_cluster(
                cls.ganesha_servers_hostname,
                cls.vips_in_nfs_ganesha_cluster)
            if not ret:
                g.log.error("Creation of nfs-ganesha cluster failed")
                return False

        if not is_nfs_ganesha_cluster_in_healthy_state(
                cls.servers_in_nfs_ganesha_cluster[0]):
            g.log.error("Nfs-ganesha cluster is not healthy")
            return False
        g.log.info("Nfs-ganesha Cluster exists is in healthy state")

        ret = configure_ports_on_clients(cls.clients)
        if not ret:
            g.log.error("Failed to configure ports on clients")
            return False

        ret = ganesha_client_firewall_settings(cls.clients)
        if not ret:
            g.log.error("Failed to do firewall setting in clients")
            return False

        for server in cls.servers:
            for client in cls.clients:
                cmd = ("if [ -z \"$(grep -R \"%s\" /etc/hosts)\" ]; then "
                       "echo \"%s %s\" >> /etc/hosts; fi"
                       % (client, socket.gethostbyname(client), client))
                ret, _, _ = g.run(server, cmd)
                if ret != 0:
                    g.log.error("Failed to add entry of client %s in "
                                "/etc/hosts of server %s"
                                % (client, server))

        for client in cls.clients:
            for server in cls.servers:
                cmd = ("if [ -z \"$(grep -R \"%s\" /etc/hosts)\" ]; then "
                       "echo \"%s %s\" >> /etc/hosts; fi"
                       % (server, socket.gethostbyname(server), server))
                ret, _, _ = g.run(client, cmd)
                if ret != 0:
                    g.log.error("Failed to add entry of server %s in "
                                "/etc/hosts of client %s"
                                % (server, client))
        return True

    @classmethod
    def tearDownClass(cls, delete_nfs_ganesha_cluster=True):
        """Teardown nfs ganesha cluster.
        """
        GlusterBaseClass.tearDownClass.im_func(cls)

        if delete_nfs_ganesha_cluster:
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
        else:
            g.log.info("Skipping teardown nfs-ganesha cluster...")


class NfsGaneshaVolumeBaseClass(NfsGaneshaClusterSetupClass):
    """Sets up the nfs ganesha cluster, volume for testing purposes.
    """
    @classmethod
    def setUpClass(cls):
        """Setup volume exports volume with nfs-ganesha,
            mounts the volume.
        """
        # pylint: disable=too-many-branches
        NfsGaneshaClusterSetupClass.setUpClass.im_func(cls)

        # Peer probe servers
        ret = peer_probe_servers(cls.mnode, cls.servers)
        if not ret:
            raise ExecutionError("Failed to peer probe servers")

        g.log.info("All peers are in connected state")

        # Peer Status from mnode
        peer_status(cls.mnode)

        for server in cls.servers:
            mount_info = [
                {'protocol': 'glusterfs',
                 'mountpoint': '/run/gluster/shared_storage',
                 'server': server,
                 'client': {'host': server},
                 'volname': 'gluster_shared_storage',
                 'options': ''}]

            mount_obj = create_mount_objs(mount_info)
            if not mount_obj[0].is_mounted():
                ret = mount_obj[0].mount()
                if not ret:
                    raise ExecutionError("Unable to mount volume '%s:%s' "
                                         "on '%s:%s'"
                                         % (mount_obj.server_system,
                                            mount_obj.volname,
                                            mount_obj.client_system,
                                            mount_obj.mountpoint))

        # Setup Volume
        ret = setup_volume(mnode=cls.mnode,
                           all_servers_info=cls.all_servers_info,
                           volume_config=cls.volume)
        if not ret:
            raise ExecutionError("Setup volume %s failed", cls.volume)
        time.sleep(10)

        # Export volume with nfs ganesha, if it is not exported already
        vol_option = get_volume_options(cls.mnode, cls.volname,
                                        option='ganesha.enable')
        if vol_option is None:
            raise ExecutionError("Failed to get ganesha.enable volume option "
                                 "for %s " % cls.volume)
        if vol_option['ganesha.enable'] != 'on':
            ret, _, _ = export_nfs_ganesha_volume(
                mnode=cls.mnode, volname=cls.volname)
            if ret != 0:
                raise ExecutionError("Failed to export volume %s "
                                     "as NFS export", cls.volname)
            time.sleep(5)

        ret = wait_for_nfs_ganesha_volume_to_get_exported(cls.mnode,
                                                          cls.volname)
        if not ret:
            raise ExecutionError("Failed to export volume %s. volume is "
                                 "not listed in showmount" % cls.volname)
        else:
            g.log.info("Volume %s is exported successfully"
                       % cls.volname)

        # Log Volume Info and Status
        ret = log_volume_info_and_status(cls.mnode, cls.volname)
        if not ret:
            raise ExecutionError("Logging volume %s info and status failed",
                                 cls.volname)

        # Create Mounts
        _rc = True
        for mount_obj in cls.mounts:
            ret = mount_obj.mount()
            if not ret:
                g.log.error("Unable to mount volume '%s:%s' on '%s:%s'",
                            mount_obj.server_system, mount_obj.volname,
                            mount_obj.client_system, mount_obj.mountpoint)
                _rc = False
        if not _rc:
            raise ExecutionError("Mounting volume %s on few clients failed",
                                 cls.volname)

        # Get info of mount before the IO
        log_mounts_info(cls.mounts)

    @classmethod
    def tearDownClass(cls, umount_vol=True, cleanup_vol=True,
                      teardown_nfs_ganesha_cluster=True):
        """Teardown the export, mounts and volume.
        """
        # pylint: disable=too-many-branches
        # Unmount volume
        if umount_vol:
            _rc = True
            for mount_obj in cls.mounts:
                ret = mount_obj.unmount()
                if not ret:
                    g.log.error("Unable to unmount volume '%s:%s' on '%s:%s'",
                                mount_obj.server_system, mount_obj.volname,
                                mount_obj.client_system, mount_obj.mountpoint)
                    _rc = False
            if not _rc:
                raise ExecutionError("Unmount of all mounts are not "
                                     "successful")

        # Cleanup volume
        if cleanup_vol:

            volinfo = get_volume_info(cls.mnode, cls.volname)
            if volinfo is None or cls.volname not in volinfo:
                g.log.info("Volume %s does not exist in %s"
                           % (cls.volname, cls.mnode))
            else:
                # Unexport volume, if it is not unexported already
                vol_option = get_volume_options(cls.mnode, cls.volname,
                                                option='ganesha.enable')
                if vol_option is None:
                    raise ExecutionError("Failed to get ganesha.enable volume "
                                         " option for %s " % cls.volume)
                if vol_option['ganesha.enable'] != 'off':
                    if is_volume_exported(cls.mnode, cls.volname, "nfs"):
                        ret, _, _ = unexport_nfs_ganesha_volume(
                            mnode=cls.mnode, volname=cls.volname)
                        if ret != 0:
                            raise ExecutionError("Failed to unexport volume %s"
                                                 % cls.volname)
                        time.sleep(5)
                else:
                    g.log.info("Volume %s is unexported already"
                               % cls.volname)

                _, _, _ = g.run(cls.mnode, "showmount -e")

            ret = cleanup_volume(mnode=cls.mnode, volname=cls.volname)
            if not ret:
                raise ExecutionError("cleanup volume %s failed", cls.volname)

        # All Volume Info
        volume_info(cls.mnode)

        (NfsGaneshaClusterSetupClass.
         tearDownClass.
         im_func(cls,
                 delete_nfs_ganesha_cluster=teardown_nfs_ganesha_cluster))


class NfsGaneshaIOBaseClass(NfsGaneshaVolumeBaseClass):
    """ Nfs Ganesha IO base class to run the tests when IO is in progress """

    @classmethod
    def setUpClass(cls):

        NfsGaneshaVolumeBaseClass.setUpClass.im_func(cls)

        # Upload io scripts for running IO on mounts
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, script_local_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts")

        cls.counter = 1

    def setUp(self):
        """setUp starts the io from all the mounts.
            IO creates deep dirs and files.
        """

        NfsGaneshaVolumeBaseClass.setUp.im_func(self)

        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 15 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 10 %s" % (self.script_upload_path,
                                             self.counter,
                                             mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            self.counter = self.counter + 10
        self.io_validation_complete = False

        # Adding a delay of 15 seconds before test method starts. This
        # is to ensure IO's are in progress and giving some time to fill data
        time.sleep(15)

    def tearDown(self):
        """If test method failed before validating IO, tearDown waits for the
        IO's to complete and checks for the IO exit status
        """

        # Wait for IO to complete if io validation is not executed in the
        # test method
        if not self.io_validation_complete:
            g.log.info("Wait for IO to complete as IO validation did not "
                       "succeed in test method")
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")
            g.log.info("IO is successful on all mounts")
        NfsGaneshaVolumeBaseClass.tearDown.im_func(self)

    @classmethod
    def tearDownClass(cls, umount_volume=True, cleanup_volume=True,
                      teardown_nfsganesha_cluster=True):
        """Cleanup data from mount, cleanup volume and delete nfs ganesha
           cluster.
        """
        # Log Mounts info
        g.log.info("Log mounts info")
        log_mounts_info(cls.mounts)

        (NfsGaneshaVolumeBaseClass.
         tearDownClass.
         im_func(cls,
                 umount_vol=umount_volume, cleanup_vol=cleanup_volume,
                 teardown_nfs_ganesha_cluster=teardown_nfsganesha_cluster))


def wait_for_nfs_ganesha_volume_to_get_exported(mnode, volname, timeout=120):
    """Waits for the nfs ganesha volume to get exported

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): volume name

    Kwargs:
        timeout (int): timeout value in seconds to wait for volume
            to get exported

    Returns:
        True on success, False otherwise

    Examples:
        >>> wait_for_volume_to_get_exported("abc.com", "testvol")
    """
    count = 0
    flag = 0
    while count < timeout:
        if is_volume_exported(mnode, volname, "nfs"):
            flag = 1
            break

        time.sleep(10)
        count = count + 10
    if not flag:
        g.log.error("Failed to export volume %s" % volname)
        return False

    return True


def wait_for_nfs_ganesha_volume_to_get_unexported(mnode, volname, timeout=120):
    """Waits for the nfs ganesha volume to get unexported

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): volume name

    Kwargs:
        timeout (int): timeout value in seconds to wait for volume
            to get unexported

    Returns:
        True on success, False otherwise

    Examples:
        >>> wait_for_volume_to_get_unexported("abc.com", "testvol")
    """
    count = 0
    flag = 0
    while count < timeout:
        if not is_volume_exported(mnode, volname, "nfs"):
            flag = 1
            break

        time.sleep(10)
        count = count + 10
    if not flag:
        g.log.error("Failed to unexport volume %s" % volname)
        return False

    return True
