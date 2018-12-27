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

"""
    Description: Library for samba operations.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.volume_libs import is_volume_exported
from glustolibs.gluster.mount_ops import GlusterMount


def start_smb_service(mnode):
    """Start smb service on the specified node.

    Args:
        mnode (str): Node on which smb service has to be started

    Returns:
        bool: True on successfully starting smb service. False otherwise.
    """
    g.log.info("Starting SMB Service on %s", mnode)

    # Enable Samba to start on boot
    ret, _, _ = g.run(mnode, "chkconfig smb on")
    if ret != 0:
        g.log.error("Unable to set chkconfig smb on")
        return False
    g.log.info("chkconfig smb on successful")

    # Start smb service
    ret, _, _ = g.run(mnode, "service smb start")
    if ret != 0:
        g.log.error("Unable to start the smb service")
        return False
    g.log.info("Successfully started smb service")

    return True


def smb_service_status(mnode):
    """Status of smb service on the specified node.

    Args:
        mnode (str): Node on which smb service has to be started

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.
    """
    g.log.info("Getting SMB Service status on %s", mnode)
    return g.run(mnode, "service smb status")


def is_smb_service_running(mnode):
    """Check if smb service is running on node

    Args:
        mnode (str): Node on which smb service status has to be verified.

    Returns:
        bool: True if smb service is running. False otherwise.
    """
    g.log.info("Check if SMB service is running on %s", mnode)
    ret, out, _ = smb_service_status(mnode)
    if ret != 0:
        return False
    if "Active: active (running)" in out:
        return True
    else:
        return False


def stop_smb_service(mnode):
    """Stop smb service on the specified node.

    Args:
        mnode (str): Node on which smb service has to be stopped.

    Returns:
        bool: True on successfully stopping smb service. False otherwise.
    """
    g.log.info("Stopping SMB Service on %s", mnode)

    # Disable Samba to start on boot
    ret, _, _ = g.run(mnode, "chkconfig smb off")
    if ret != 0:
        g.log.error("Unable to set chkconfig smb off")
        return False
    g.log.info("chkconfig smb off successful")

    # Stop smb service
    ret, _, _ = g.run(mnode, "service smb stop")
    if ret != 0:
        g.log.error("Unable to stop the smb service")
        return False
    g.log.info("Successfully stopped smb service")

    return True


def list_smb_shares(mnode):
    """List all the gluster volumes that are exported as SMB Shares

    Args:
        mnode (str): Node on which commands has to be executed.

    Returns:
        list: List of all volume names that are exported as SMB Shares.
             Empty list if no volumes are exported as SMB Share.
    """
    g.log.info("List all SMB Shares")
    smb_shares_list = []
    cmd = "smbclient -L localhost"
    ret, out, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Failed to find the SMB Shares")
        return smb_shares_list
    else:
        out = out.splitlines()
        for line in out:
            if 'gluster-' in line:
                smb_shares_list.append(line.split(" ")[0].strip())

    return smb_shares_list


def enable_mounting_volume_over_smb(mnode, volname, smb_users_info):
    """Enable mounting volume over SMB. Set ACL's for non-root users.

    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume on which acl's has to be set.
        smb_users_info (dict): Dict containing users info. Example:
            smb_users_info = {
                'root': {'password': 'foobar',
                         'acl': ''
                         },
                'user1': {'password': 'abc',
                          'acl': ''
                          },
                'user2': {'password': 'xyz',
                          'acl': ''
                          }
                }
    Returns:
        bool: True on successfully enabling to mount volume using SMB.
            False otherwise.
    """
    g.log.info("Enable mounting volume over SMB")
    # Create a temp mount to provide required permissions to the smb user
    mount = {
        'protocol': 'glusterfs',
        'server': mnode,
        'volname': volname,
        'client': {
            'host': mnode
            },
        'mountpoint': '/tmp/gluster_smb_set_user_permissions_%s' % volname,
        'options': 'acl'
        }
    mount_obj = GlusterMount(mount)
    ret = mount_obj.mount()
    if not ret:
        g.log.error("Unable to create temporary mount for providing "
                    "required permissions to the smb users")
        return False
    g.log.info("Successfully created temporary mount for providing "
               "required permissions to the smb users")

    # Provide required permissions to the smb user
    for smb_user in smb_users_info.keys():
        if smb_user != 'root':
            if 'acl' in smb_users_info[smb_user]:
                acl = smb_users_info[smb_user]['acl']
                if not acl:
                    acl = "rwx"
            else:
                acl = "rwx"

            cmd = ("setfacl -m user:%s:%s %s" % (smb_user, acl,
                                                 mount_obj.mountpoint))
            ret, _, _ = g.run(mnode, cmd)
            if ret != 0:
                g.log.error("Unable to provide required permissions to the "
                            "smb user %s ", smb_user)
                return False
            g.log.info("Successfully provided required permissions to the "
                       "smb user %s ", smb_user)

    # Verify SMB/CIFS share  can be accessed by the user

    # Unmount the temp mount created
    ret = mount_obj.unmount()
    if not ret:
        g.log.error("Unable to unmount the temp mount")
    g.log.info("Successfully unmounted the temp mount")

    return True


def share_volume_over_smb(mnode, volname, smb_users_info):
    """Sharing volumes over SMB

    Args:
        mnode (str): Node on which commands has to be executed.
        volname (str): Name of the volume to be shared.
        smb_users_info (dict): Dict containing users info. Example:
            smb_users_info = {
                'root': {'password': 'foobar',
                         'acl': ''
                         },
                'user1': {'password': 'abc',
                          'acl': ''
                          },
                'user2': {'password': 'xyz',
                          'acl': ''
                          }
                }

    Returns:
        bool : True on successfully sharing the volume over SMB.
            False otherwise
    """
    g.log.info("Start sharing the volume over SMB")

    # Set volume option 'user.cifs' to 'on'.
    cmd = "gluster volume set %s user.cifs on" % volname
    ret, _, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Failed to set the volume option user.cifs on")
        return False
    g.log.info("Successfully set 'user.cifs' to 'on' on %s", volname)

    # Set volume option 'stat-prefetch' to 'on'.
    cmd = "gluster volume set %s stat-prefetch on" % volname
    ret, _, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Failed to set the volume option stat-prefetch on")
        return False
    g.log.info("Successfully set 'stat-prefetch' to 'on' on %s", volname)

    # Set volume option 'server.allow-insecure' to 'on'.
    cmd = "gluster volume set %s server.allow-insecure on" % volname
    ret, _, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Failed to set the volume option server-allow-insecure")
        return False
    g.log.info("Successfully set 'server-allow-insecure' to 'on' on %s",
               volname)

    # Set 'storage.batch-fsync-delay-usec' to 0.
    # This is to ensure ping_pong's lock and I/O coherency tests works on CIFS.
    cmd = ("gluster volume set %s storage.batch-fsync-delay-usec 0" % volname)
    ret, _, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Failed to set the volume option "
                    "'storage.batch-fsync-delay-usec' to 0 on %s", volname)
        return False
    g.log.info("Successfully set 'storage.batch-fsync-delay-usec' to 0 on %s",
               volname)

    # Verify if the volume can be accessed from the SMB/CIFS share.
    cmd = ("smbclient -L localhost -U | grep -i -Fw gluster-%s " % volname)
    ret, _, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("volume '%s' not accessible via SMB/CIFS share", volname)
        return False
    g.log.info("volume '%s' can be accessed from SMB/CIFS share", volname)

    # To verify if the SMB/CIFS share can be accessed by the root/non-root user
    # TBD

    # Enable mounting volumes over SMB
    ret = enable_mounting_volume_over_smb(mnode, volname, smb_users_info)
    if not ret:
        g.log.error("Failed to enable mounting volumes using SMB")
        return False
    g.log.info("Successfully enabled mounting volumes using SMV for the "
               "smbusers: %s", str(smb_users_info.keys()))

    # Verify if volume is shared
    ret = is_volume_exported(mnode, volname, "smb")
    if not ret:
        g.log.info("Volume %s is not exported as 'cifs/smb' share", volname)
        return False
    g.log.info("Volume %s is exported as 'cifs/smb' share", volname)

    return True
