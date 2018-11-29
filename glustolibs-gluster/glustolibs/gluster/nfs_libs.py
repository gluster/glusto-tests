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
    Description: Libraries for gluster nfs operations.
"""
import time
from glusto.core import Glusto as g
from glustolibs.gluster.volume_libs import is_volume_exported


def export_volume_through_nfs(mnode, volname, enable_ganesha=False,
                              time_delay=30):
    """Export the volume through nfs

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
        enable_ganesha (bool): Enable ganesha for the volume.
        time_delay (int): Time to wait after the volume set operations
            to validate whether the volume is exported or not.

    Returns:
        bool: If volume is successfully exported through nfs returns True.
            False Otherwise.
    """
    # Enable ganesha on the volume if enable_ganesha is True
    if enable_ganesha:
        cmd = ("gluster volume set %s ganesha.enable on --mode=script" %
               volname)
        ret, _, _ = g.run(mnode, cmd)
        if ret != 0:
            g.log.error("Failed to enable nfs ganesha for volume %s", volname)
            return False
    else:
        # Enable nfs on the volume
        cmd = ("gluster volume set %s nfs.disable off --mode=script" % volname)
        ret, _, _ = g.run(mnode, cmd)
        if ret != 0:
            g.log.error("Failed to enable nfs for the volume %s", volname)
            return False

    time.sleep(time_delay)
    # Verify if volume is exported
    ret = is_volume_exported(mnode, volname, "nfs")
    if not ret:
        g.log.info("Volume %s is not exported as 'nfs' export", volname)
        return False

    return True
