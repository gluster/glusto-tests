""" This Module demostrates how to use functions available in volume_ops
"""
from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import (
    GlusterBaseClass,
    runs_on,
)


@runs_on([['distributed-replicated', 'replicated'],
          ['glusterfs', 'nfs']])
class DemoGlusterBaseClassVariables(GlusterBaseClass):
    """Demonstrating all the functions available in volume_ops module
    """
    @classmethod
    def setUpClass(cls):
        """
        """
        # Read all the cluster config from the g.config and assign it to
        # class variables
        cls.get_super_method(cls, 'setUpClass')()

        # Servers (list)
        g.log.info("Servers:\n %s\n\n", cls.servers)

        # Clients (list)
        g.log.info("Clients:\n %s\n\n", cls.clients)

        # Servers Info
        g.log.info("Servers Info:\n %s\n\n", cls.all_servers_info)

        # Clients Info
        g.log.info("Clients Info:\n %s\n\n", cls.all_clients_info)

        # Server IP's
        g.log.info("Servers IP's:\n %s\n\n", cls.servers_ips)

        # Volume type
        g.log.info("Volume Type: %s\n\n", cls.volume_type)

        # Mount type
        g.log.info("Mount Type: %s\n\n", cls.mount_type)

        # SMB Cluster info
        g.log.info("SMB Users Info:\n %s\n\n", cls.smb_users_info)

        # NFS-Ganesha Cluster info
        g.log.info("NFS-Ganesha  Number of Nodes:\n %s\n\n",
                   cls.num_of_nfs_ganesha_nodes)

        # Default volume_types configuration
        g.log.info("Default volume_types configuration:\n %s\n\n",
                   cls.default_volume_type_config)

        # Volume configuration
        g.log.info("Volume configuration:\n %s\n\n", cls.volume)

        # Volume options
        g.log.info("Default Volume Options:\n %s\n\n", cls.volume_options)

        # Mnode
        g.log.info("Mnode: %s\n\n", cls.mnode)

        # Mounts
        g.log.info("Mounts:\n %s\n\n", cls.mounts)

        # Gluster log dirs
        g.log.info("Gluster Log dirs:\n%s\n\n", cls.server_gluster_logs_dirs)

        # Gluster Log files
        g.log.info("Gluster Log files:\n%s\n\n", cls.client_gluster_logs_files)

    def test1(self):
        pass
