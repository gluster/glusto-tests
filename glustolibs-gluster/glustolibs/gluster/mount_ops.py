#!/usr/bin/env python
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
    Description: Module for Mount operations.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.windows_libs import powershell
import copy


class GlusterMount():
    """Gluster Mount class

    Args:
        mount (dict): Mount dict with mount_protocol, mountpoint,
            server, client, volname, options, smbuser, smbpasswd,
            platform, super_user as keys

            Note: smbuser, smbpasswd are applicable for windows mounts.

            client is a dict with host, super_user, platform as keys.

                platform should be specified in case of windows. By default
                it is assumed to be linux.

                super_user is 'root' by default.
                In case of windows the super_user can be the user who has
                all the admin previliges.

        Example:
            mount =
                {'mount_protocol': 'glusterfs',
                 'mountpoint': '/mnt/g1',
                 'server': 'abc.lab.eng.xyz.com',
                 'client': {'host': 'def.lab.eng.xyz.com'},
                 'volname': 'testvoi',
                 'options': ''
                 }

            mount =
                {'mount_protocol': 'nfs',
                 'mountpoint': '/mnt/n1',
                 'server': 'abc.lab.eng.xyz.com',
                 'client': {'host': 'def.lab.eng.xyz.com'},
                 'volname': 'testvoi',
                 'options': ''}

            mount =
                {'mount_protocol': 'smb',
                 'mountpoint': '',
                 'server': 'abc.lab.eng.xyz.com',
                 'client': {
                     'host': 'def.lab.eng.xyz.com',
                     'super_user': 'Admin'
                     },
                 'volname': 'testvoi',
                 'options': '',
                 'smbuser': 'abc',
                 'smbpasswd': 'def'}
    Returns:
        Instance of GlusterMount class
   """
    def __init__(self, mount):
        self.mounttype = None
        self.mountpoint = None
        self.server_system = None
        self.client_system = None
        self.volname = None
        self.options = ''
        self.smbuser = None
        self.smbpasswd = None
        self.user = None
        self.platform = None

        # Get Protocol
        if 'protocol' in mount:
            self.mounttype = mount['protocol']
        else:
            self.mounttype = "glusterfs"

        # Get mountpoint
        mount_point_defined = False
        if 'mountpoint' in mount:
            if mount['mountpoint']:
                mount_point_defined = True

        if mount_point_defined:
            self.mountpoint = mount['mountpoint']
        else:
            if self.mounttype == "smb":
                self.mountpoint = "*"
            else:
                self.mountpoint = "/mnt/%s" % self.mounttype

        # Get server
        self.server_system = mount['server']

        # Get client
        self.client_system = mount['client']['host']

        # Get super_user
        user_defined = False
        if 'super_user' in mount['client']:
            if mount['client']['super_user']:
                self.user = mount['client']['super_user']
                user_defined = True

        if not user_defined:
            self.user = "root"

        # Get platform
        platform_defined = False
        if 'platform' in mount['client']:
            if mount['client']['platform']:
                self.platform = mount['client']['platform']
                platform_defined = True

        if not platform_defined:
            self.platform = 'linux'

        # Get Volume name
        self.volname = mount['volname']

        # Get options
        if 'options' in mount:
            self.options = mount['options']

        # If mounttype is 'smb' or 'cifs' get 'smbuser' and 'smbpassword'
        if self.mounttype == 'smb' or self.mounttype == 'cifs':
            if 'smbuser' in mount:
                if mount['smbuser']:
                    self.smbuser = mount['smbuser']

            if 'smbpasswd' in mount:
                if mount['smbpasswd']:
                    self.smbpasswd = mount['smbpasswd']

    def mount(self):
        """Mounts the volume

        Args:
            uses instance args passed at init

        Returns:
            bool: True on success and False on failure.
        """
        ret, out, err = mount_volume(self.volname, mtype=self.mounttype,
                                     mpoint=self.mountpoint,
                                     mserver=self.server_system,
                                     mclient=self.client_system,
                                     options=self.options,
                                     smbuser=self.smbuser,
                                     smbpasswd=self.smbpasswd,
                                     user=self.user)
        if ret != 0:
            return False
        else:
            if self.mounttype == "smb":
                self.mountpoint = out.strip()
        return True

    def is_mounted(self):
        """Tests for mount on client

        Args:
            uses instance args passed at init

        Returns:
            bool: True on success and False on failure.
        """
        ret = is_mounted(self.volname,
                         mpoint=self.mountpoint,
                         mserver=self.server_system,
                         mclient=self.client_system,
                         mtype=self.mounttype,
                         user=self.user)

        if ret:
            return True
        else:
            return False

    def unmount(self):
        """Unmounts the volume

        Args:
            uses instance args passed at init

        Returns:
            bool: True on success and False on failure.
        """
        (ret, out, err) = umount_volume(mclient=self.client_system,
                                        mpoint=self.mountpoint,
                                        mtype=self.mounttype,
                                        user=self.user)
        rc = True
        if ret == 0:
            if self.mounttype == "smb":
                if not (('deleted successfully' in out) or
                        ('command completed successfully' in out) or
                        ('There are no entries in the list' in out) or
                        ('The network connection could not be found')):
                    rc = False
                else:
                    self.mountpoint = "*"
        else:
            rc = False

        return rc


def is_mounted(volname, mpoint, mserver, mclient, mtype, user='root'):
    """Check if mount exist.

    Args:
        volname (str): Name of the volume
        mpoint (str): Mountpoint dir
        mserver (str): Server to which it is mounted to
        mclient (str): Client from which it is mounted.
        mtype (str): Mount type (glusterfs|nfs|smb|cifs)

    Kwargs:
        user (str): Super user of the node mclient

    Returns:
        bool: True if mounted and False otherwise.
    """
    # python will error on missing arg, so just checking for empty args here
    if not volname or not mpoint or not mserver or not mclient or not mtype:
        g.log.error("Missing arguments for mount.")
        return False

    if mtype == "smb":
        if mpoint == "*":
            return False
        else:
            cmd = powershell("net use %s" % mpoint)
            ret, out, err = g.run(mclient, cmd, user)
            if ret != 0:
                return False
            else:
                expected_output = ("Remote name       \\\%s\gluster-%s" %
                                   (mserver, volname))
                if expected_output in out:
                    return True
                else:
                    return False
    else:
        ret, _, _ = g.run(mclient, "mount | grep %s | grep %s | grep \"%s\""
                          % (volname, mpoint, mserver), user)
        if ret == 0:
            g.log.debug("Volume %s is mounted at %s:%s" % (volname, mclient,
                                                           mpoint))
            return True
        else:
            g.log.error("Volume %s is not mounted at %s:%s" % (volname,
                                                               mclient,
                                                               mpoint))
            return False


def mount_volume(volname, mtype, mpoint, mserver, mclient, options='',
                 smbuser=None, smbpasswd=None, user='root'):
    """Mount the gluster volume with specified options.

    Args:
        volname (str): Name of the volume to mount.
        mtype (str): Protocol to be used to mount.
        mpoint (str): Mountpoint dir.
        mserver (str): Server to mount.
        mclient (str): Client from which it has to be mounted.

    Kwargs:
        option (str): Options for the mount command.
        smbuser (str): SMB USERNAME. Used with mtype = 'cifs'
        smbpasswd (str): SMB PASSWD. Used with mtype = 'cifs'
        user (str): Super user of the node mclient


    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            (0, '', '') if already mounted.
            (1, '', '') if setup_samba_service fails in case of smb.
            (ret, out, err) of mount commnd execution otherwise.
    """
    if is_mounted(volname, mpoint, mserver, mclient, mtype, user):
        g.log.debug("Volume %s is already mounted at %s" %
                    (volname, mpoint))
        return (0, '', '')

    if options != '':
        options = "-o %s" % options

    if mtype == 'smb':
        if smbuser is None or smbpasswd is None:
            g.log.error("smbuser and smbpasswd to be passed as parameters "
                        "for cifs mounts")
            return (1, '', '')

        mcmd = ("net use %s \\\\%s\\gluster-%s " % (mpoint, mserver, volname) +
                " /user:%s " % (smbuser) + '"' + smbpasswd + '"')

        mcmd = powershell(mcmd)

        ret, out, err = g.run(mclient, mcmd, user=user)
        if ret != 0:
            g.log.error("net use comand failed on windows client %s "
                        "failed: %s" % (mclient, err))
            return (ret, out, err)

        if out.startswith('Drive'):
            drv_ltr = out.split(' ')[1]
            g.log.info("Samba share mount success on windows client %s. "
                       "Share is : %s" % (mclient, drv_ltr))
            return (ret, drv_ltr, err)

        g.log.error("net use comand successful but error in mount of samba "
                    " share for windows client %s for reason %s" %
                    (mclient, err))
        return (1, out, err)

    if mtype == 'nfs':
        if not options:
            options = "-o vers=3"

        elif options and 'vers' not in options:
            options = options + ",vers=3"

    mcmd = ("mount -t %s %s %s:/%s %s" %
            (mtype, options, mserver, volname, mpoint))

    if mtype == 'cifs':
        if smbuser is None or smbpasswd is None:
            g.log.error("smbuser and smbpasswd to be passed as parameters "
                        "for cifs mounts")
            return (1, '', '')

        mcmd = ("mount -t cifs -o username=%s,password=%s "
                "\\\\\\\\%s\\\\gluster-%s %s" % (smbuser, smbpasswd, mserver,
                                                 volname, mpoint))

    # Create mount dir
    _, _, _ = g.run(mclient, "test -d %s || mkdir -p %s" % (mpoint, mpoint),
                    user=user)

    # Create mount
    return g.run(mclient, mcmd, user=user)


def umount_volume(mclient, mpoint, mtype='', user='root'):
    """Unmounts the mountpoint.

    Args:
        mclient (str): Client from which it has to be mounted.
        mpoint (str): Mountpoint dir.

    Kwargs:
        mtype (str): Mounttype. Defaults to ''.
        user (str): Super user of the node mclient. Defaults to 'root'

    Returns:
        tuple: Tuple containing three elements (ret, out, err) as returned by
            umount command execution.
    """
    if mtype == "smb":
        cmd = "net use %s /d /Y" % mpoint
        cmd = powershell(cmd)
    else:
        cmd = ("umount %s || umount -f %s || umount -l %s" %
               (mpoint, mpoint, mpoint))
    return g.run(mclient, cmd, user=user)


def create_mount_objs(mounts):
    """Creates GlusterMount class objects for the given list of mounts

    Args:
        mounts (list): list of mounts with each element being dict having the
            specifics of each mount

        Example:
            mounts: [
                {'protocol': 'glusterfs',
                 'mountpoint': '/mnt/g1',
                 'server': 'abc.lab.eng.xyz.com',
                 'client': {'host': 'def.lab.eng.xyz.com'},
                 'volname': 'testvoi',
                 'options': '',
                 'num_of_mounts': 2},

                {'protocol': 'nfs',
                 'mountpoint': '/mnt/n1',
                 'server': 'abc.lab.eng.xyz.com',
                 'client': {'host': 'def.lab.eng.xyz.com'},
                 'volname': 'testvoi',
                 'options': ''}

                {'protocol': 'smb',
                 'mountpoint': '',
                 'server': 'abc.lab.eng.xyz.com',
                 'client': {
                     'host': 'def.lab.eng.xyz.com',
                     'super_user': 'Admin'
                     },
                 'volname': 'testvoi',
                 'options': '',
                 'smbuser': 'abc',
                 'smbpasswd': 'def',
                 'num_of_mounts': 2}
                ]
    Returns:
        list : List of GlusterMount class objects.

    Example:
        mount_objs = create_mount_objs(mounts)
    """
    mount_obj_list = []
    for mount in mounts:
        temp_mount = copy.deepcopy(mount)
        if (mount['protocol'] == "glusterfs" or mount['protocol'] == "nfs" or
                mount['protocol'] == "cifs"):
            if 'mountpoint' in mount and mount['mountpoint']:
                temp_mount['mountpoint'] = mount['mountpoint']
            else:
                temp_mount['mountpoint'] = ("/mnt/%s_%s" %
                                            (mount['volname'],
                                             mount['protocol']))
        elif mount['protocol'] == "smb":
            if 'mountpoint' in mount and mount['mountpoint']:
                temp_mount['mountpoint'] = mount['mountpoint']
            else:
                temp_mount['mountpoint'] = "*"

        num_of_mounts = 1
        if 'num_of_mounts' in mount:
            if mount['num_of_mounts']:
                num_of_mounts = mount['num_of_mounts']
        if num_of_mounts > 1:
            mount_dir = temp_mount['mountpoint']
            for count in range(1, num_of_mounts + 1):
                if mount_dir != "*":
                    temp_mount['mountpoint'] = '_'.join(
                        [mount_dir, str(count)])

                mount_obj_list.append(GlusterMount(temp_mount))
        else:
            mount_obj_list.append(GlusterMount(temp_mount))

    return mount_obj_list


def create_mounts(mount_objs):
    """Creates Mounts using the details as specified in the each mount obj

    Args:
        mount_objs (list): list of mounts objects with each element being
            the GlusterMount class object

    Returns:
        bool : True if creating the mount for all mount_objs is successful.
            False otherwise.

    Example:
        ret = create_mounts(create_mount_objs(mounts))
    """
    rc = True
    for mount_obj in mount_objs:
        ret = mount_obj.mount()
        if not ret:
            rc = False
    return rc


def unmount_mounts(mount_objs):
    """Creates Mounts using the details as specified in the each mount obj

    Args:
        mount_objs (list): list of mounts objects with each element being
            the GlusterMount class object

    Returns:
        bool : True if unmounting the mount for all mount_objs is successful.
            False otherwise.

    Example:
        ret = unmount_mounts(create_mount_objs(mounts))
    """
    rc = True
    for mount_obj in mount_objs:
        ret = mount_obj.unmount()
        if not ret:
            rc = False
    return rc
