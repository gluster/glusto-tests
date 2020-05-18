#!/usr/bin/env python
#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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
        Samba ctdb base classes.
    Pre-requisite:
        Please install samba ctdb packages
        on all servers
"""

from glusto.core import Glusto as g
from glustolibs.gluster.ctdb_ops import (
    edit_hook_script,
    enable_ctdb_cluster,
    create_nodes_file,
    create_public_address_file,
    start_ctdb_service,
    is_ctdb_status_healthy,
    teardown_samba_ctdb_cluster)
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import (
    setup_volume,
    wait_for_volume_process_to_be_online)


class SambaCtdbBaseClass(GlusterBaseClass):
    """
    Creates samba ctdb cluster
    """
    @classmethod
    def setUpClass(cls):
        """
        Setup variable for samba ctdb test.
        """
        super(SambaCtdbBaseClass, cls).setUpClass()

        cls.ctdb_volume_rep_count = int(len(cls.ctdb_nodes))
        cls.primary_node = cls.servers[0]
        g.log.info("VOLUME REP COUNT %s", cls.ctdb_volume_rep_count)

        cls.ctdb_vips = (g.config['gluster']['cluster_config']
                         ['smb']['ctdb_vips'])
        cls.ctdb_nodes = (g.config['gluster']['cluster_config']
                          ['smb']['ctdb_nodes'])
        cls.ctdb_volname = (g.config['gluster']['cluster_config']
                            ['smb']['ctdb_volname'])
        cls.ctdb_volume_config = (g.config['gluster']['cluster_config']['smb']
                                  ['ctdb_volume_config'])

    @classmethod
    def setup_samba_ctdb_cluster(cls):
        """
        Create ctdb-samba cluster if doesn't exists

        Returns:
            bool: True if successfully setup samba else false
        """
        # Check if ctdb setup is up and running
        if is_ctdb_status_healthy(cls.primary_node):
            g.log.info("ctdb setup already up skipping "
                       "ctdb setup creation")
            return True
        g.log.info("Proceeding with ctdb setup creation")
        for mnode in cls.servers:
            ret = edit_hook_script(mnode, cls.ctdb_volname)
            if not ret:
                return False
            ret = enable_ctdb_cluster(mnode)
            if not ret:
                return False
            ret = create_nodes_file(mnode, cls.ctdb_nodes)
            if not ret:
                return False
            ret = create_public_address_file(mnode, cls.ctdb_vips)
            if not ret:
                return False
        server_info = cls.all_servers_info
        ctdb_config = cls.ctdb_volume_config
        g.log.info("Setting up ctdb volume %s", cls.ctdb_volname)
        ret = setup_volume(mnode=cls.primary_node,
                           all_servers_info=server_info,
                           volume_config=ctdb_config)
        if not ret:
            g.log.error("Failed to setup ctdb volume %s", cls.ctdb_volname)
            return False
        g.log.info("Successful in setting up volume %s", cls.ctdb_volname)

        # Wait for volume processes to be online
        g.log.info("Wait for volume %s processes to be online",
                   cls.ctdb_volname)
        ret = wait_for_volume_process_to_be_online(cls.mnode, cls.ctdb_volname)
        if not ret:
            g.log.error("Failed to wait for volume %s processes to "
                        "be online", cls.ctdb_volname)
            return False
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", cls.ctdb_volname)

        # start ctdb services
        ret = start_ctdb_service(cls.servers)
        if not ret:
            return False

        ret = is_ctdb_status_healthy(cls.primary_node)
        if not ret:
            g.log.error("CTDB setup creation failed - exiting")
            return False
        g.log.info("CTDB setup creation successfull")
        return True

    @classmethod
    def tearDownClass(cls, delete_samba_ctdb_cluster=False):
        """
        Teardown samba ctdb cluster.
        """
        super(SambaCtdbBaseClass, cls).tearDownClass()

        if delete_samba_ctdb_cluster:
            ret = teardown_samba_ctdb_cluster(
                cls.servers, cls.ctdb_volname)
            if not ret:
                raise ExecutionError("Cleanup of samba ctdb "
                                     "cluster failed")
            g.log.info("Teardown samba ctdb cluster succeeded")
        else:
            g.log.info("Skipping teardown samba ctdb cluster...")
