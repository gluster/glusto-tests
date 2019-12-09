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
    configure_ports_on_clients,
    ganesha_client_firewall_settings)
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.exceptions import ExecutionError, ConfigError
from glustolibs.gluster.volume_libs import is_volume_exported


class NfsGaneshaClusterSetupClass(GlusterBaseClass):
    """Creates nfs ganesha cluster
    """
    @classmethod
    def setUpClass(cls):
        """
        Setup variable for nfs-ganesha tests.
        """
        # pylint: disable=too-many-statements, too-many-branches
        super(NfsGaneshaClusterSetupClass, cls).setUpClass()

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
        super(NfsGaneshaClusterSetupClass, cls).tearDownClass()

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
