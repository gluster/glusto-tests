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

""" Description: Module for gluster volume related helper functions. """

import time
import random
try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree
from glusto.core import Glusto as g
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.volume_ops import (volume_create, volume_start,
                                           set_volume_options, get_volume_info,
                                           volume_stop, volume_delete,
                                           volume_info, volume_status,
                                           get_volume_options,
                                           get_volume_list)
from glustolibs.gluster.tiering_ops import (add_extra_servers_to_cluster,
                                            tier_attach,
                                            is_tier_process_running)
from glustolibs.gluster.quota_ops import (enable_quota, set_quota_limit_usage,
                                          is_quota_enabled)
from glustolibs.gluster.uss_ops import enable_uss, is_uss_enabled
from glustolibs.gluster.snap_ops import snap_delete_by_volumename
from glustolibs.gluster.heal_libs import (
    are_all_self_heal_daemons_are_online,
    wait_for_self_heal_daemons_to_be_online)
from glustolibs.gluster.brick_ops import add_brick, remove_brick, replace_brick


def volume_exists(mnode, volname):
    """Check if volume already exists

    Args:
        mnode (str): Node on which commands has to be executed
        volname (str): Name of the volume.

    Returns:
        NoneType: If there are errors
        bool : True if volume exists. False Otherwise
    """
    volume_list = get_volume_list(mnode)
    if volume_list is None:
        g.log.error("'gluster volume list' on node %s Failed", mnode)
        return None

    if volname in volume_list:
        return True
    else:
        return False


def setup_volume(mnode, all_servers_info, volume_config, force=False):
    """Setup Volume with the configuration defined in volume_config

    Args:
        mnode (str): Node on which commands has to be executed
        all_servers_info (dict): Information about all servers.
        example :
            all_servers_info = {
                'abc.lab.eng.xyz.com': {
                    'host': 'abc.lab.eng.xyz.com',
                    'brick_root': '/bricks',
                    'devices': ['/dev/vdb', '/dev/vdc', '/dev/vdd', '/dev/vde']
                    },
                'def.lab.eng.xyz.com':{
                    'host': 'def.lab.eng.xyz.com',
                    'brick_root': '/bricks',
                    'devices': ['/dev/vdb', '/dev/vdc', '/dev/vdd', '/dev/vde']
                    }
                }
        volume_config (dict): Dict containing volume information
        example :
            volume_config = {
                'name': 'testvol',
                'servers': ['server-vm1', 'server-vm2', 'server-vm3',
                            'server-vm4'],
                'voltype': {'type': 'distributed',
                            'dist_count': 4,
                            'transport': 'tcp'},
                'extra_servers': ['server-vm9', 'server-vm10',
                                  'server-vm11', 'server-vm12'],
                'quota': {'limit_usage': {'path': '/', 'percent': None,
                                          'size': '100GB'},
                          'enable': False},
                'uss': {'enable': False},
                'tier': {'create_tier': True,
                         'tier_type': {'type': 'distributed-replicated',
                                  'replica_count': 2,
                                  'dist_count': 2,
                                  'transport': 'tcp'}},
                'options': {'performance.readdir-ahead': True}
                }
    Returns:
        bool : True on successful setup. False Otherwise

    """
    # Get volume name
    if 'name' in volume_config:
        volname = volume_config['name']
    else:
        g.log.error("Unable to get the volume name from config")
        return False

    # Check if the volume already exists
    volinfo = get_volume_info(mnode=mnode)
    if volinfo is not None and volname in volinfo.keys():
        g.log.info("volume %s already exists. Returning...", volname)
        return True

    # Get servers
    if 'servers' in volume_config:
        servers = volume_config['servers']
    else:
        g.log.error("Unable to get the volume servers from config")
        return False

    # Get the volume type and values
    if not ('voltype' in volume_config and 'type' in volume_config['voltype']):
        g.log.error("Voltype not defined in config for the volume %s",
                    volname)
        return False

    volume_type = volume_config['voltype']['type']
    kwargs = {}
    number_of_bricks = 1
    if volume_type == 'distributed':
        if 'dist_count' in volume_config['voltype']:
            kwargs['dist_count'] = (volume_config['voltype']['dist_count'])
        else:
            g.log.error("Distibute Count not specified in the volume config")
            return False

        number_of_bricks = kwargs['dist_count']

    elif volume_type == 'replicated':
        if 'replica_count' in volume_config['voltype']:
            kwargs['replica_count'] = (volume_config['voltype']
                                       ['replica_count'])
        else:
            g.log.error("Replica count not specified in the volume config")
            return False

        if 'arbiter_count' in volume_config['voltype']:
            kwargs['arbiter_count'] = (volume_config['voltype']
                                       ['arbiter_count'])

        number_of_bricks = kwargs['replica_count']

    elif volume_type == 'distributed-replicated':
        if 'dist_count' in volume_config['voltype']:
            kwargs['dist_count'] = (volume_config['voltype']['dist_count'])
        else:
            g.log.error("Distibute Count not specified in the volume config")
            return False

        if 'replica_count' in volume_config['voltype']:
            kwargs['replica_count'] = (volume_config['voltype']
                                       ['replica_count'])
        else:
            g.log.error("Replica count not specified in the volume config")
            return False

        if 'arbiter_count' in volume_config['voltype']:
            kwargs['arbiter_count'] = (volume_config['voltype']
                                       ['arbiter_count'])

        number_of_bricks = (kwargs['dist_count'] * kwargs['replica_count'])

    elif volume_type == 'dispersed':
        if 'disperse_count' in volume_config['voltype']:
            kwargs['disperse_count'] = (volume_config['voltype']
                                        ['disperse_count'])
        else:
            g.log.error("Disperse Count not specified in the volume config")
            return False

        if 'redundancy_count' in volume_config['voltype']:
            kwargs['redundancy_count'] = (volume_config['voltype']
                                          ['redundancy_count'])
        else:
            g.log.error("Redunduncy Count not specified in the volume config")
            return False

        number_of_bricks = kwargs['disperse_count']

    elif volume_type == 'distributed-dispersed':
        if 'dist_count' in volume_config['voltype']:
            kwargs['dist_count'] = (volume_config['voltype']['dist_count'])
        else:
            g.log.error("Distibute Count not specified in the volume config")
            return False

        if 'disperse_count' in volume_config['voltype']:
            kwargs['disperse_count'] = (volume_config['voltype']
                                        ['disperse_count'])
        else:
            g.log.error("Disperse Count not specified in the volume config")
            return False

        if 'redundancy_count' in volume_config['voltype']:
            kwargs['redundancy_count'] = (volume_config['voltype']
                                          ['redundancy_count'])
        else:
            g.log.error("Redunduncy Count not specified in the volume config")
            return False

        number_of_bricks = (kwargs['dist_count'] * kwargs['disperse_count'])
    else:
        g.log.error("Invalid volume type defined in config")
        return False

    # get bricks_list
    bricks_list = form_bricks_list(mnode=mnode, volname=volname,
                                   number_of_bricks=number_of_bricks,
                                   servers=servers,
                                   servers_info=all_servers_info)
    if not bricks_list:
        g.log.error("Number_of_bricks is greater than the unused bricks on "
                    "servers")
        return False

    # Create volume
    ret, _, _ = volume_create(mnode=mnode, volname=volname,
                              bricks_list=bricks_list, force=force,
                              **kwargs)
    if ret != 0:
        g.log.error("Unable to create volume %s", volname)
        return False

    # Start Volume
    time.sleep(2)
    ret = volume_start(mnode, volname)
    if not ret:
        g.log.error("volume start %s failed", volname)
        return False

    # Create Tier volume
    if ('tier' in volume_config and 'create_tier' in volume_config['tier'] and
            volume_config['tier']['create_tier']):
        # get servers info for tier attach
        if ('extra_servers' in volume_config and
                volume_config['extra_servers']):
            extra_servers = volume_config['extra_servers']
            ret = add_extra_servers_to_cluster(mnode, extra_servers)
            if not ret:
                return False
        else:
            extra_servers = volume_config['servers']

        # get the tier volume type
        if 'tier_type' in volume_config['tier']:
            if 'type' in volume_config['tier']['tier_type']:
                tier_volume_type = volume_config['tier']['tier_type']['type']
                dist = rep = 1
                if tier_volume_type == 'distributed':
                    if 'dist_count' in volume_config['tier']['tier_type']:
                        dist = (volume_config['tier']['tier_type']
                                ['dist_count'])

                elif tier_volume_type == 'replicated':
                    if 'replica_count' in volume_config['tier']['tier_type']:
                        rep = (volume_config['tier']['tier_type']
                               ['replica_count'])

                elif tier_volume_type == 'distributed-replicated':
                    if 'dist_count' in volume_config['tier']['tier_type']:
                        dist = (volume_config['tier']['tier_type']
                                ['dist_count'])
                    if 'replica_count' in volume_config['tier']['tier_type']:
                        rep = (volume_config['tier']['tier_type']
                               ['replica_count'])
        else:
            tier_volume_type = 'distributed'
            dist = 1
            rep = 1
        number_of_bricks = dist * rep

        # Attach Tier
        ret, _, _ = tier_attach(mnode=mnode, volname=volname,
                                extra_servers=extra_servers,
                                extra_servers_info=all_servers_info,
                                num_bricks_to_add=number_of_bricks,
                                replica=rep)
        if ret != 0:
            g.log.error("Unable to attach tier")
            return False

        time.sleep(30)
        # Check if tier is running
        _rc = True
        for server in extra_servers:
            ret = is_tier_process_running(server, volname)
            if not ret:
                g.log.error("Tier process not running on %s", server)
                _rc = False
        if not _rc:
            return False

    # Enable Quota
    if ('quota' in volume_config and 'enable' in volume_config['quota'] and
            volume_config['quota']['enable']):
        ret, _, _ = enable_quota(mnode=mnode, volname=volname)
        if ret != 0:
            g.log.error("Unable to set quota on the volume %s", volname)
            return False

        # Check if 'limit_usage' is defined
        if 'limit_usage' in volume_config['quota']:
            if 'path' in volume_config['quota']['limit_usage']:
                path = volume_config['quota']['limit_usage']['path']
            else:
                path = "/"

            if 'size' in volume_config['quota']['limit_usage']:
                size = volume_config['quota']['limit_usage']['size']
            else:
                size = "100GB"
        else:
            path = "/"
            size = "100GB"

        # Set quota_limit_usage
        ret, _, _ = set_quota_limit_usage(mnode=mnode, volname=volname,
                                          path=path, limit=size)
        if ret != 0:
            g.log.error("Unable to set quota limit on the volume %s", volname)
            return False

        # Check if quota is enabled
        ret = is_quota_enabled(mnode=mnode, volname=volname)
        if not ret:
            g.log.error("Quota not enabled on the volume %s", volname)
            return False

    # Enable USS
    if ('uss' in volume_config and 'enable' in volume_config['uss'] and
            volume_config['uss']['enable']):
        ret, _, _ = enable_uss(mnode=mnode, volname=volname)
        if ret != 0:
            g.log.error("Unable to enable uss on the volume %s", volname)
            return False

        ret = is_uss_enabled(mnode=mnode, volname=volname)
        if not ret:
            g.log.error("USS is not enabled on the volume %s", volname)
            return False

    # Set all the volume options:
    if 'options' in volume_config:
        volume_options = volume_config['options']
        ret = set_volume_options(mnode=mnode, volname=volname,
                                 options=volume_options)
        if not ret:
            g.log.error("Unable to set few volume options")
            return False
    return True


def cleanup_volume(mnode, volname):
    """deletes snapshots in the volume, stops and deletes the gluster
       volume if given volume exists in gluster and deletes the
       directories in the bricks associated with the given volume

    Args:
        volname (str): volume name
        mnode (str): Node on which cmd has to be executed.

    Returns:
        bool: True, if volume is deleted successfully
              False, otherwise

    Example:
        cleanup_volume("abc.xyz.com", "testvol")
    """
    volinfo = get_volume_info(mnode, volname)
    if volinfo is None or volname not in volinfo:
        g.log.info("Volume %s does not exist in %s", volname, mnode)
        return True

    ret, _, _ = snap_delete_by_volumename(mnode, volname)
    if ret != 0:
        g.log.error("Failed to delete the snapshots in volume %s", volname)
        return False

    ret, _, _ = volume_stop(mnode, volname, force=True)
    if ret != 0:
        g.log.error("Failed to stop volume %s", volname)
        return False

    ret = volume_delete(mnode, volname)
    if not ret:
        g.log.error("Unable to cleanup the volume %s", volname)
        return False
    return True


def is_volume_exported(mnode, volname, share_type):
    """Checks whether the volume is exported as nfs or cifs share

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
        share_type (str): nfs or cifs

    Returns:
        bool: If volume is exported returns True. False Otherwise.
    """
    if 'nfs' in share_type:
        cmd = "showmount -e localhost"
        _, _, _ = g.run(mnode, cmd)

        cmd = "showmount -e localhost | grep -w %s" % volname
        ret, _, _ = g.run(mnode, cmd)
        if ret != 0:
            return False
        else:
            return True

    if 'cifs' in share_type or 'smb' in share_type:
        cmd = "smbclient -L localhost"
        _, _, _ = g.run(mnode, cmd)

        cmd = ("smbclient -L localhost -U | grep -i -Fw gluster-%s " %
               volname)
        ret, _, _ = g.run(mnode, cmd)
        if ret != 0:
            return False
        else:
            return True
    return True


def log_volume_info_and_status(mnode, volname):
    """Logs volume info and status

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Returns:
        bool: Returns True if getting volume info and status is successful.
            False Otherwise.
    """
    ret, _, _ = volume_info(mnode, volname)
    if ret != 0:
        return False

    ret, _, _ = volume_status(mnode, volname)
    if ret != 0:
        return False

    return True


def verify_all_process_of_volume_are_online(mnode, volname):
    """Verifies whether all the processes of volume are online

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Returns:
        bool: Returns True if all the processes of volume are online.
            False Otherwise.
    """
    from glustolibs.gluster.brick_libs import are_bricks_online, get_all_bricks
    # Verify all the  brick process are online
    bricks_list = get_all_bricks(mnode, volname)
    if not bricks_list:
        return False

    ret = are_bricks_online(mnode, volname, bricks_list)
    if not ret:
        return False

    # Verify all self-heal-daemons are running for non-distribute volumes.
    if not is_distribute_volume(mnode, volname):
        ret = are_all_self_heal_daemons_are_online(mnode, volname)
        if not ret:
            return False

    return True


def get_subvols(mnode, volname):
    """Gets the subvolumes in the given volume

    Args:
        volname (str): volume name
        mnode (str): Node on which cmd has to be executed.

    Returns:
        dict: with empty list values for all keys, if volume doesn't exist
        dict: Dictionary of subvols, value of each key is list of lists
            containing subvols
    Example:
        get_subvols("abc.xyz.com", "testvol")
    """

    subvols = {
        'is_tier': False,
        'hot_tier_subvols': [],
        'cold_tier_subvols': [],
        'volume_subvols': []
        }
    volinfo = get_volume_info(mnode, volname)
    if volinfo is not None:
        voltype = volinfo[volname]['typeStr']
        if voltype == 'Tier':
            # Set is_tier to True
            subvols['is_tier'] = True

            # Get hot tier subvols
            hot_tier_type = (volinfo[volname]["bricks"]
                             ['hotBricks']['hotBrickType'])
            tmp = volinfo[volname]["bricks"]['hotBricks']["brick"]
            hot_tier_bricks = [x["name"] for x in tmp if "name" in x]
            if hot_tier_type == 'Distribute':
                for brick in hot_tier_bricks:
                    subvols['hot_tier_subvols'].append([brick])

            elif (hot_tier_type == 'Replicate' or
                  hot_tier_type == 'Distributed-Replicate'):
                rep_count = int(
                    (volinfo[volname]["bricks"]['hotBricks']
                     ['numberOfBricks']).split("=", 1)[0].split("x")[1].strip()
                    )
                subvol_list = (
                    [hot_tier_bricks[i:i + rep_count]
                     for i in range(0, len(hot_tier_bricks), rep_count)])
                subvols['hot_tier_subvols'] = subvol_list

            # Get cold tier subvols
            cold_tier_type = (volinfo[volname]["bricks"]['coldBricks']
                              ['coldBrickType'])
            tmp = volinfo[volname]["bricks"]['coldBricks']["brick"]
            cold_tier_bricks = [x["name"] for x in tmp if "name" in x]

            # Distribute volume
            if cold_tier_type == 'Distribute':
                for brick in cold_tier_bricks:
                    subvols['cold_tier_subvols'].append([brick])

            # Replicate or Distribute-Replicate volume
            elif (cold_tier_type == 'Replicate' or
                  cold_tier_type == 'Distributed-Replicate'):
                rep_count = int(
                    (volinfo[volname]["bricks"]['coldBricks']
                     ['numberOfBricks']).split("=", 1)[0].split("x")[1].strip()
                    )
                subvol_list = (
                    [cold_tier_bricks[i:i + rep_count]
                     for i in range(0, len(cold_tier_bricks), rep_count)])
                subvols['cold_tier_subvols'] = subvol_list

            # Disperse or Distribute-Disperse volume
            elif (cold_tier_type == 'Disperse' or
                  cold_tier_type == 'Distributed-Disperse'):
                disp_count = sum(
                    [int(nums) for nums in (
                        (volinfo[volname]["bricks"]['coldBricks']
                         ['numberOfBricks']).split("x", 1)[1].
                        strip().split("=")[0].strip().strip("()").
                        split()) if nums.isdigit()])
                subvol_list = [cold_tier_bricks[i:i + disp_count]
                               for i in range(0, len(cold_tier_bricks),
                                              disp_count)]
                subvols['cold_tier_subvols'] = subvol_list
            return subvols

        tmp = volinfo[volname]["bricks"]["brick"]
        bricks = [x["name"] for x in tmp if "name" in x]
        if voltype == 'Replicate' or voltype == 'Distributed-Replicate':
            rep_count = int(volinfo[volname]['replicaCount'])
            subvol_list = [bricks[i:i + rep_count]for i in range(0,
                                                                 len(bricks),
                                                                 rep_count)]
            subvols['volume_subvols'] = subvol_list
        elif voltype == 'Distribute':
            for brick in bricks:
                subvols['volume_subvols'].append([brick])

        elif voltype == 'Disperse' or voltype == 'Distributed-Disperse':
            disp_count = int(volinfo[volname]['disperseCount'])
            subvol_list = ([bricks[i:i + disp_count]
                            for i in range(0, len(bricks), disp_count)])
            subvols['volume_subvols'] = subvol_list
    return subvols


def is_tiered_volume(mnode, volname):
    """Check if volume is tiered volume.

    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.

    Returns:
        bool : True if the volume is tiered volume. False otherwise
        NoneType: None if volume doesnot exist.
    """
    volinfo = get_volume_info(mnode, volname)
    if volinfo is None:
        g.log.error("Unable to get the volume info for volume %s", volname)
        return None

    voltype = volinfo[volname]['typeStr']
    if voltype == 'Tier':
        return True
    else:
        return False


def is_distribute_volume(mnode, volname):
    """Check if volume is a plain distributed volume

    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.

    Returns:
        bool : True if the volume is distributed volume. False otherwise
        NoneType: None if volume doesnot exist.
    """
    volume_type_info = get_volume_type_info(mnode, volname)
    if volume_type_info is None:
        g.log.error("Unable to check if the volume %s is distribute", volname)
        return False

    if volume_type_info['is_tier']:
        hot_tier_type = (volume_type_info['hot_tier_type_info']
                         ['hotBrickType'])
        cold_tier_type = (volume_type_info['cold_tier_type_info']
                          ['coldBrickType'])
        if hot_tier_type == 'Distribute' and cold_tier_type == 'Distribute':
            return True
        else:
            return False
    else:
        if volume_type_info['volume_type_info']['typeStr'] == 'Distribute':
            return True
        else:
            return False


def get_volume_type_info(mnode, volname):
    """Returns volume type information for the specified volume.

    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.

    Retunrs:
        dict : Dict containing the keys, values defining the volume type:
            Example:
                volume_type_info = {
                    'is_tier': False,
                    'hot_tier_type_info': {},
                    'cold_tier_type_info': {},
                    'volume_type_info': {
                        'typeStr': 'Disperse',
                        'replicaCount': '1',
                        'arbiterCount': '0',
                        'stripeCount': '1',
                        'disperseCount': '3',
                        'redundancyCount': '1'
                        }
                    }

                volume_type_info = {
                    'is_tier': True,
                    'hot_tier_type_info': {
                        'hotBrickType': 'Distribute',
                        'hotreplicaCount': '1'
                        },
                    'cold_tier_type_info': {
                        'coldBrickType': 'Disperse',
                        'coldreplicaCount': '1',
                        'coldarbiterCount': '0',
                        'colddisperseCount': '3',
                        'numberOfBricks':1
                        },
                    'volume_type_info': {}


        NoneType: None if volume does not exist or any other key errors.
    """
    volinfo = get_volume_info(mnode, volname)
    if volinfo is None:
        g.log.error("Unable to get the volume info for volume %s", volname)
        return None

    volume_type_info = {
        'is_tier': False,
        'hot_tier_type_info': {},
        'cold_tier_type_info': {},
        'volume_type_info': {}
        }

    voltype = volinfo[volname]['typeStr']
    if voltype == 'Tier':
        volume_type_info['is_tier'] = True

        hot_tier_type_info = get_hot_tier_type_info(mnode, volname)
        volume_type_info['hot_tier_type_info'] = hot_tier_type_info

        cold_tier_type_info = get_cold_tier_type_info(mnode, volname)
        volume_type_info['cold_tier_type_info'] = cold_tier_type_info

    else:
        non_tiered_volume_type_info = {
            'typeStr': '',
            'replicaCount': '',
            'arbiterCount': '',
            'stripeCount': '',
            'disperseCount': '',
            'redundancyCount': ''
            }
        for key in non_tiered_volume_type_info.keys():
            if key in volinfo[volname]:
                non_tiered_volume_type_info[key] = volinfo[volname][key]
            else:
                g.log.error("Unable to find key '%s' in the volume info for "
                            "the volume %s", key, volname)
                non_tiered_volume_type_info[key] = None
        volume_type_info['volume_type_info'] = non_tiered_volume_type_info

    return volume_type_info


def get_cold_tier_type_info(mnode, volname):
    """Returns cold tier type information for the specified volume.

    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.

    Retunrs:
        dict : Dict containing the keys, values defining the cold tier type:
            Example:
                cold_tier_type_info = {
                    'coldBrickType': 'Disperse',
                    'coldreplicaCount': '1',
                    'coldarbiterCount': '0',
                    'colddisperseCount': '3',
                    'numberOfBricks': '3'
                    }
        NoneType: None if volume does not exist or is not a tiered volume or
            any other key errors.
    """
    volinfo = get_volume_info(mnode, volname)
    if volinfo is None:
        g.log.error("Unable to get the volume info for volume %s", volname)
        return None

    if not is_tiered_volume(mnode, volname):
        g.log.error("Volume %s is not a tiered volume", volname)
        return None

    cold_tier_type_info = {
        'coldBrickType': '',
        'coldreplicaCount': '',
        'coldarbiterCount': '',
        'colddisperseCount': '',
        'numberOfBricks': ''
        }
    for key in cold_tier_type_info.keys():
        if key in volinfo[volname]['bricks']['coldBricks']:
            cold_tier_type_info[key] = (volinfo[volname]['bricks']
                                        ['coldBricks'][key])
        else:
            g.log.error("Unable to find key '%s' in the volume info for the "
                        "volume %s", key, volname)
            return None

    if 'Disperse' in cold_tier_type_info['coldBrickType']:
        redundancy_count = (cold_tier_type_info['numberOfBricks'].
                            split("x", 1)[1].strip().
                            split("=")[0].strip().strip("()").split()[2])
        cold_tier_type_info['coldredundancyCount'] = redundancy_count

    return cold_tier_type_info


def get_hot_tier_type_info(mnode, volname):
    """Returns hot tier type information for the specified volume.

    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.

    Retunrs:
        dict : Dict containing the keys, values defining the hot tier type:
            Example:
                hot_tier_type_info = {
                    'hotBrickType': 'Distribute',
                    'hotreplicaCount': '1'
                    }
        NoneType: None if volume does not exist or is not a tiered volume or
            any other key errors.
    """
    volinfo = get_volume_info(mnode, volname)
    if volinfo is None:
        g.log.error("Unable to get the volume info for volume %s", volname)
        return None

    if not is_tiered_volume(mnode, volname):
        g.log.error("Volume %s is not a tiered volume", volname)
        return None

    hot_tier_type_info = {
        'hotBrickType': '',
        'hotreplicaCount': ''
        }
    for key in hot_tier_type_info.keys():
        if key in volinfo[volname]['bricks']['hotBricks']:
            hot_tier_type_info[key] = (volinfo[volname]['bricks']['hotBricks']
                                       [key])
        else:
            g.log.error("Unable to find key '%s' in the volume info for the "
                        "volume %s", key, volname)
            return None

    return hot_tier_type_info


def get_num_of_bricks_per_subvol(mnode, volname):
    """Returns number of bricks per subvol

    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.

    Returns:
        dict : Dict containing the keys, values defining
                number of bricks per subvol
                Example:
                    num_of_bricks_per_subvol = {
                        'is_tier': False,
                        'hot_tier_num_of_bricks_per_subvol': None,
                        'cold_tier_num_of_bricks_per_subvol': None,
                        'volume_num_of_bricks_per_subvol': 2
                        }

                    num_of_bricks_per_subvol = {
                        'is_tier': True,
                        'hot_tier_num_of_bricks_per_subvol': 3,
                        'cold_tier_num_of_bricks_per_subvol': 2,
                        'volume_num_of_bricks_per_subvol': None
                        }

        NoneType: None if volume doesnot exist or is a tiered volume.
    """
    bricks_per_subvol_dict = {
        'is_tier': False,
        'hot_tier_num_of_bricks_per_subvol': None,
        'cold_tier_num_of_bricks_per_subvol': None,
        'volume_num_of_bricks_per_subvol': None
        }

    subvols_dict = get_subvols(mnode, volname)
    if subvols_dict['volume_subvols']:
        bricks_per_subvol_dict['volume_num_of_bricks_per_subvol'] = (
            len(subvols_dict['volume_subvols'][0]))
    else:
        if (subvols_dict['hot_tier_subvols'] and
                subvols_dict['cold_tier_subvols']):
            bricks_per_subvol_dict['is_tier'] = True
            bricks_per_subvol_dict['hot_tier_num_of_bricks_per_subvol'] = (
                len(subvols_dict['hot_tier_subvols'][0]))
            bricks_per_subvol_dict['cold_tier_num_of_bricks_per_subvol'] = (
                len(subvols_dict['cold_tier_subvols'][0]))

    return bricks_per_subvol_dict


def get_cold_tier_num_of_bricks_per_subvol(mnode, volname):
    """Returns number of bricks per subvol in cold tier

    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.

    Returns:
        int : Number of bricks per subvol on cold tier.
        NoneType: None if volume doesnot exist or not a tiered volume.
    """
    if not is_tiered_volume(mnode, volname):
        g.log.error("Volume %s is not a tiered volume", volname)
        return None
    subvols_dict = get_subvols(mnode, volname)
    if subvols_dict['cold_tier_subvols']:
        return len(subvols_dict['cold_tier_subvols'][0])
    else:
        return None


def get_hot_tier_num_of_bricks_per_subvol(mnode, volname):
    """Returns number of bricks per subvol in hot tier

    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.

    Returns:
        int : Number of bricks per subvol on hot tier.
        NoneType: None if volume doesnot exist or not a tiered volume.
    """
    if not is_tiered_volume(mnode, volname):
        g.log.error("Volume %s is not a tiered volume", volname)
        return None
    subvols_dict = get_subvols(mnode, volname)
    if subvols_dict['hot_tier_subvols']:
        return len(subvols_dict['hot_tier_subvols'][0])
    else:
        return None


def get_replica_count(mnode, volname):
    """Get the replica count of the volume

    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.

    Returns:
        dict : Dict contain keys, values defining Replica count of the volume.
            Example:
                replica_count_info = {
                    'is_tier': False,
                    'hot_tier_replica_count': None,
                    'cold_tier_replica_count': None,
                    'volume_replica_count': 3
                    }
                replica_count_info = {
                    'is_tier': True,
                    'hot_tier_replica_count': 2,
                    'cold_tier_replica_count': 3,
                    'volume_replica_count': None
                    }
        NoneType: None if it is parse failure.
    """
    vol_type_info = get_volume_type_info(mnode, volname)
    if vol_type_info is None:
        g.log.error("Unable to get the replica count info for the volume %s",
                    volname)
        return None

    replica_count_info = {
        'is_tier': False,
        'hot_tier_replica_count': None,
        'cold_tier_replica_count': None,
        'volume_replica_count': None
        }

    replica_count_info['is_tier'] = vol_type_info['is_tier']
    if replica_count_info['is_tier']:
        replica_count_info['hot_tier_replica_count'] = (
            vol_type_info['hot_tier_type_info']['hotreplicaCount'])
        replica_count_info['cold_tier_replica_count'] = (
            vol_type_info['cold_tier_type_info']['coldreplicaCount'])

    else:
        replica_count_info['volume_replica_count'] = (
            vol_type_info['volume_type_info']['replicaCount'])

    return replica_count_info


def get_cold_tier_replica_count(mnode, volname):
    """Get the replica count of cold tier.

    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.

    Returns:
        int : Replica count of the cold tier.
        NoneType: None if volume does not exist or not a tiered volume.
    """
    is_tier = is_tiered_volume(mnode, volname)
    if not is_tier:
        return None
    else:
        volinfo = get_volume_info(mnode, volname)
        cold_tier_replica_count = (volinfo[volname]["bricks"]['coldBricks']
                                   ['coldreplicaCount'])
        return cold_tier_replica_count


def get_hot_tier_replica_count(mnode, volname):
    """Get the replica count of hot tier.

    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.

    Returns:
        int : Replica count of the hot tier.
        NoneType: None if volume does not exist or not a tiered volume.
    """
    is_tier = is_tiered_volume(mnode, volname)
    if not is_tier:
        return None
    else:
        volinfo = get_volume_info(mnode, volname)
        hot_tier_replica_count = (volinfo[volname]["bricks"]['hotBricks']
                                  ['hotreplicaCount'])
        return hot_tier_replica_count


def get_disperse_count(mnode, volname):
    """Get the disperse count of the volume

    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.

    Returns:
        dict : Dict contain keys, values defining Disperse count of the volume.
            Example:
                disperse_count_info = {
                    'is_tier': False,
                    'cold_tier_disperse_count': None,
                    'volume_disperse_count': 3
                    }
                disperse_count_info = {
                    'is_tier': True,
                    'cold_tier_disperse_count': 3,
                    'volume_disperse_count': None
                    }
        None: If it is non dispersed volume.
    """
    vol_type_info = get_volume_type_info(mnode, volname)
    if vol_type_info is None:
        g.log.error("Unable to get the disperse count info for the volume %s",
                    volname)
        return None

    disperse_count_info = {
        'is_tier': False,
        'cold_tier_disperse_count': None,
        'volume_disperse_count': None
        }

    disperse_count_info['is_tier'] = vol_type_info['is_tier']
    if disperse_count_info['is_tier']:
        disperse_count_info['cold_tier_disperse_count'] = (
            vol_type_info['cold_tier_type_info']['colddisperseCount'])

    else:
        disperse_count_info['volume_disperse_count'] = (
            vol_type_info['volume_type_info']['disperseCount'])

    return disperse_count_info


def get_cold_tier_disperse_count(mnode, volname):
    """Get the disperse count of cold tier.

    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.

    Returns:
        int : disperse count of the cold tier.
        NoneType: None if volume does not exist or not a tiered volume.
    """
    is_tier = is_tiered_volume(mnode, volname)
    if not is_tier:
        return None
    else:
        volinfo = get_volume_info(mnode, volname)
        cold_tier_disperse_count = (volinfo[volname]["bricks"]['coldBricks']
                                    ['colddisperseCount'])
        return cold_tier_disperse_count


def enable_and_validate_volume_options(mnode, volname, volume_options_list,
                                       time_delay=5):
    """Enable the volume option and validate whether the option has be
    successfully enabled or not

    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.
        volume_options_list (str|list): A volume option|List of volume options
            to be enabled
        time_delay (int): Time delay between 2 volume set operations

    Returns:
        bool: True when enabling and validating all volume options is
            successful. False otherwise
    """
    if isinstance(volume_options_list, str):
        volume_options_list = [volume_options_list]

    for option in volume_options_list:
        # Set volume option to 'enable'
        g.log.info("Setting the volume option : %s", option)
        ret = set_volume_options(mnode, volname, {option: "enable"})
        if not ret:
            return False

        # Validate whether the option is set on the volume
        g.log.info("Validating the volume option : %s to be set to 'enable'",
                   option)
        option_dict = get_volume_options(mnode, volname, option)
        g.log.info("Options Dict: %s", option_dict)
        if option_dict is None:
            g.log.error("%s is not enabled on the volume %s", option, volname)
            return False

        if option not in option_dict or "enable" not in option_dict[option]:
            g.log.error("%s is not enabled on the volume %s", option, volname)
            return False

        g.log.info("%s is enabled on the volume %s", option, volname)
        time.sleep(time_delay)

    return True


def form_bricks_list_to_add_brick(mnode, volname, servers, all_servers_info,
                                  add_to_hot_tier=False,
                                  **kwargs):
    """Forms list of bricks to add-bricks to the volume.

    Args:
        mnode (str): Node on which commands has to be executed
        volname (str): volume name
        servers (list): List of servers in the storage pool.
        all_servers_info (dict): Information about all servers.
        example :
            all_servers_info = {
                'abc.lab.eng.xyz.com': {
                    'host': 'abc.lab.eng.xyz.com',
                    'brick_root': '/bricks',
                    'devices': ['/dev/vdb', '/dev/vdc', '/dev/vdd', '/dev/vde']
                    },
                'def.lab.eng.xyz.com':{
                    'host': 'def.lab.eng.xyz.com',
                    'brick_root': '/bricks',
                    'devices': ['/dev/vdb', '/dev/vdc', '/dev/vdd', '/dev/vde']
                    }
                }
    Kwargs:
        add_to_hot_tier (bool): True If bricks are to be added to hot_tier.
            False otherwise. Defaults to False.

        The keys, values in kwargs are:
            - replica_count : (int)|None.
              Increase the current_replica_count by replica_count
            - distribute_count: (int)|None.
              Increase the current_distribute_count by distribute_count

    Returns:
        list: List of bricks to add if there are enough bricks to add on
            the servers.
        nonetype: None if there are not enough bricks to add on the servers or
            volume doesn't exists or any other failure.
    """

    # Check if volume exists
    if not volume_exists(mnode, volname):
        g.log.error("Volume %s doesn't exists.", volname)
        return None

    # Check if the volume has to be expanded by n distribute count.
    distribute_count = None
    if 'distribute_count' in kwargs:
        distribute_count = int(kwargs['distribute_count'])

    # Check whether we need to increase the replica count of the volume
    replica_count = None
    if 'replica_count' in kwargs:
        replica_count = int(kwargs['replica_count'])

    if replica_count is None and distribute_count is None:
        distribute_count = 1

    # Check if the volume has to be expanded by n distribute count.
    num_of_distribute_bricks_to_add = 0
    if distribute_count:
        # Get Number of bricks per subvolume.
        bricks_per_subvol_dict = get_num_of_bricks_per_subvol(mnode, volname)

        # Get number of bricks to add.
        if bricks_per_subvol_dict['is_tier']:
            if add_to_hot_tier:
                num_of_bricks_per_subvol = (
                    bricks_per_subvol_dict['hot_tier_num_of_bricks_per_subvol']
                    )
            else:
                num_of_bricks_per_subvol = (
                    bricks_per_subvol_dict
                    ['cold_tier_num_of_bricks_per_subvol']
                    )
        else:
            num_of_bricks_per_subvol = (
                bricks_per_subvol_dict['volume_num_of_bricks_per_subvol'])

        if num_of_bricks_per_subvol is None:
            g.log.error("Number of bricks per subvol is None. "
                        "Something majorly went wrong on the volume %s",
                        volname)
            return False

        num_of_distribute_bricks_to_add = (num_of_bricks_per_subvol *
                                           distribute_count)

    # Check if the volume has to be expanded by n replica count.
    num_of_replica_bricks_to_add = 0
    if replica_count:
        # Get Subvols
        subvols_info = get_subvols(mnode, volname)

        # Calculate number of bricks to add
        if subvols_info['is_tier']:
            if add_to_hot_tier:
                num_of_subvols = len(subvols_info['hot_tier_subvols'])
            else:
                num_of_subvols = len(subvols_info['cold_tier_subvols'])
        else:
            num_of_subvols = len(subvols_info['volume_subvols'])

        if num_of_subvols == 0:
            g.log.error("No Sub-Volumes available for the volume %s."
                        " Hence cannot proceed with add-brick", volname)
            return None

        num_of_replica_bricks_to_add = replica_count * num_of_subvols

    # Calculate total number of bricks to add
    if (num_of_distribute_bricks_to_add != 0 and
            num_of_replica_bricks_to_add != 0):
        num_of_bricks_to_add = (
                num_of_distribute_bricks_to_add +
                num_of_replica_bricks_to_add +
                (distribute_count * replica_count)
            )
    else:
        num_of_bricks_to_add = (
            num_of_distribute_bricks_to_add +
            num_of_replica_bricks_to_add
        )

    # Form bricks list to add bricks to the volume.
    bricks_list = form_bricks_list(mnode=mnode, volname=volname,
                                   number_of_bricks=num_of_bricks_to_add,
                                   servers=servers,
                                   servers_info=all_servers_info)
    if not bricks_list:
        g.log.error("Number of bricks is greater than the unused bricks on "
                    "servers. Hence failed to form bricks list to "
                    "add-brick")
        return None
    else:
        return bricks_list


def expand_volume(mnode, volname, servers, all_servers_info, force=False,
                  add_to_hot_tier=False, **kwargs):
    """Forms list of bricks to add and adds those bricks to the volume.

    Args:
        mnode (str): Node on which commands has to be executed
        volname (str): volume name
        servers (str|list): A server|List of servers in the storage pool.
        all_servers_info (dict): Information about all servers.
        example :
            all_servers_info = {
                'abc.lab.eng.xyz.com': {
                    'host': 'abc.lab.eng.xyz.com',
                    'brick_root': '/bricks',
                    'devices': ['/dev/vdb', '/dev/vdc', '/dev/vdd', '/dev/vde']
                    },
                'def.lab.eng.xyz.com':{
                    'host': 'def.lab.eng.xyz.com',
                    'brick_root': '/bricks',
                    'devices': ['/dev/vdb', '/dev/vdc', '/dev/vdd', '/dev/vde']
                    }
                }
    Kwargs:
        force (bool): If this option is set to True, then add-brick command
            will get executed with force option. If it is set to False,
            then add-brick command will get executed without force option

        add_to_hot_tier (bool): True If bricks are to be added to hot_tier.
            False otherwise. Defaults to False.

        **kwargs
            The keys, values in kwargs are:
                - replica_count : (int)|None.
                    Increase the current_replica_count by replica_count
                - distribute_count: (int)|None.
                    Increase the current_distribute_count by distribute_count
                - arbiter_count : (int)|None
    Returns:
        bool: True of expanding volumes is successful.
            False otherwise.

    NOTE: adding bricks to hot tier is yet to be added in this function.
    """
    bricks_list = form_bricks_list_to_add_brick(mnode, volname, servers,
                                                all_servers_info,
                                                add_to_hot_tier, **kwargs)

    if not bricks_list:
        g.log.info("Unable to get bricks list to add-bricks. "
                   "Hence unable to expand volume : %s", volname)
        return False

    if 'replica_count' in kwargs:
        replica_count = int(kwargs['replica_count'])

        # Get replica count info.
        replica_count_info = get_replica_count(mnode, volname)

        if is_tiered_volume(mnode, volname):
            if add_to_hot_tier:
                current_replica_count = (
                    int(replica_count_info['hot_tier_replica_count']))
            else:
                current_replica_count = (
                    int(replica_count_info['cold_tier_replica_count']))
        else:
            current_replica_count = (
                int(replica_count_info['volume_replica_count']))

        kwargs['replica_count'] = current_replica_count + replica_count

    # Add bricks to the volume
    g.log.info("Adding bricks to the volume: %s", volname)
    ret, out, err = add_brick(mnode, volname, bricks_list, force=force,
                              **kwargs)
    if ret != 0:
        g.log.error("Failed to add bricks to the volume: %s", err)
        return False
    g.log.info("Successfully added bricks to the volume: %s", out)
    return True


def form_bricks_list_to_remove_brick(mnode, volname, subvol_num=None,
                                     replica_num=None,
                                     remove_from_hot_tier=False, **kwargs):
    """Form bricks list for removing the bricks.

    Args:
        mnode (str): Node on which commands has to be executed
        volname (str): volume name

    Kwargs:
        subvol_num (int|list): int|List of sub volumes number to remove.
            For example: If subvol_num = [2, 5], Then we will be removing
            bricks from 2nd and 5th sub-volume of the given volume.
            The sub-volume number starts from 0.

        replica_num (int): Specify which replica brick to remove.
            If replica_num = 0, then 1st brick from each subvolume is removed.
            the replica_num starts from 0.

        remove_from_hot_tier (bool): True If bricks are to be removed from
            hot_tier. False otherwise. Defaults to False.

        **kwargs
            The keys, values in kwargs are:
                - replica_count : (int)|None. Specify the number of replicas
                    reduce
                - distribute_count: (int)|None. Specify the distribute count to
                    reduce.
    Returns:
        list: List of bricks to remove from the volume.
        nonetype: None if volume doesn't exists or any other failure.
    """
    # Check if volume exists
    if not volume_exists(mnode, volname):
        g.log.error("Volume %s doesn't exists.", volname)
        return None

    # If distribute_count, replica_count or replica_leg , subvol_num is
    # not specified, then default shrink_volume to randomly pick
    # a subvolume to remove
    if ('distribute_count' not in kwargs and
            'replica_count' not in kwargs and
            replica_num is None and
            subvol_num is None):
        kwargs['distribute_count'] = 1

    # Get Subvols
    subvols_info = get_subvols(mnode, volname)

    # Initialize bricks to remove
    bricks_list_to_remove = []

    # remove bricks by reducing replica count of the volume
    if replica_num is not None or 'replica_count' in kwargs:
        # Get replica count info.
        replica_count_info = get_replica_count(mnode, volname)

        # Get volume type info
        volume_type_info = get_volume_type_info(mnode, volname)

        # Set is_arbiter to False
        is_arbiter = False

        # Calculate bricks to remove
        if subvols_info['is_tier']:
            if remove_from_hot_tier:
                current_replica_count = (
                    int(replica_count_info['hot_tier_replica_count']))
                subvols_list = subvols_info['hot_tier_subvols']
            else:
                current_replica_count = (
                    int(replica_count_info['cold_tier_replica_count']))
                subvols_list = subvols_info['cold_tier_subvols']
                arbiter_count = int(volume_type_info['cold_tier_type_info']
                                    ['coldarbiterCount'])
                if arbiter_count == 1:
                    is_arbiter = True
        else:
            current_replica_count = (
                int(replica_count_info['volume_replica_count']))
            subvols_list = subvols_info['volume_subvols']
            arbiter_count = int(volume_type_info['volume_type_info']
                                ['arbiterCount'])
            if arbiter_count == 1:
                is_arbiter = True

        # If replica_num is specified select the bricks of that replica number
        # from all the subvolumes.
        if replica_num is not None:
            if isinstance(replica_num, int):
                replica_num = [replica_num]

            for each_replica_num in replica_num:
                try:
                    bricks_list_to_remove.extend([subvol[each_replica_num]
                                                  for subvol in subvols_list])
                except IndexError:
                    g.log.error("Provided replica Number '%d' is greater "
                                "than or equal to the Existing replica "
                                "count '%d' of the "
                                "volume %s. Hence cannot proceed with "
                                "forming bricks for remove-brick",
                                replica_num, current_replica_count, volname)
                    return None

        # If arbiter_volume, always remove the 3rd brick (arbiter brick)
        elif is_arbiter:
            bricks_list_to_remove.extend([subvol[-1]
                                          for subvol in subvols_list])

        # If replica_num is not specified nor it is arbiter volume, randomly
        # select the bricks to remove.
        else:
            replica_count = int(kwargs['replica_count'])

            if replica_count >= current_replica_count:
                g.log.error("Provided replica count '%d' is greater than or "
                            "equal to the Existing replica count '%d' of the "
                            "volume %s. Hence cannot proceed with "
                            "forming bricks for remove-brick",
                            replica_count, current_replica_count, volname)
                return None

            sample = ([random.sample(subvol, replica_count)
                       for subvol in subvols_list])
            for item in sample:
                bricks_list_to_remove.extend(item)

    # remove bricks from sub-volumes
    if subvol_num is not None or 'distribute_count' in kwargs:
        if subvols_info['is_tier']:
            if remove_from_hot_tier:
                subvols_list = subvols_info['hot_tier_subvols']
            else:
                subvols_list = subvols_info['cold_tier_subvols']
        else:
            subvols_list = subvols_info['volume_subvols']

        if not subvols_list:
            g.log.error("No Sub-Volumes available for the volume %s", volname)
            return None

        # select bricks of subvol_num specified as argument to this function.
        if subvol_num is not None:
            if isinstance(subvol_num, int):
                subvol_num = [subvol_num]
            for each_subvol_num in subvol_num:
                try:
                    bricks_list_to_remove.extend(subvols_list[each_subvol_num])

                except IndexError:
                    g.log.error("Invalid sub volume number: %d specified "
                                "for removing the subvolume from the "
                                "volume: %s", subvol_num, volname)
                    return None

        # select bricks from multiple subvols with number of
        # subvolumes specified as distribute_count argument.
        elif 'distribute_count' in kwargs:
            distribute_count = int(kwargs['distribute_count'])
            sample = random.sample(subvols_list, distribute_count)
            for item in sample:
                bricks_list_to_remove.extend(item)

        # randomly choose a subvolume to remove-bricks from.
        else:
            bricks_list_to_remove.extend(random.choice(subvols_list))

    return list(set(bricks_list_to_remove))


def shrink_volume(mnode, volname, subvol_num=None, replica_num=None,
                  force=False, rebalance_timeout=300, delete_bricks=True,
                  remove_from_hot_tier=False, **kwargs):
    """Remove bricks from the volume.

    Args:
        mnode (str): Node on which commands has to be executed
        volname (str): volume name

    Kwargs:
        subvol_num (int|list): int|List of sub volumes number to remove.
            For example: If subvol_num = [2, 5], Then we will be removing
            bricks from 2nd and 5th sub-volume of the given volume.
            The sub-volume number starts from 0.

        replica_num (int|list): int|List of replica leg to remove.
            If replica_num = 0, then 1st brick from each subvolume is removed.
            the replica_num starts from 0.

        force (bool): If this option is set to True, then remove-brick command
            will get executed with force option. If it is set to False,
            then remove-brick is executed with 'start' and 'commit' when the
            remove-brick 'status' becomes completed.

        rebalance_timeout (int): Wait time for remove-brick to complete in
            seconds. Default is 5 minutes.

        delete_bricks (bool): After remove-brick delete the removed bricks.

        remove_from_hot_tier (bool): True If bricks are to be removed from
            hot_tier. False otherwise. Defaults to False.

        **kwargs
            The keys, values in kwargs are:
                - replica_count : (int)|None. Specify the replica count to
                    reduce
                - distribute_count: (int)|None. Specify the distribute count to
                    reduce.
    Returns:
        bool: True if removing bricks from the volume is successful.
            False otherwise.

    NOTE: remove-bricks from hot-tier is yet to be added in this function.
    """
    # Form bricks list to remove-bricks
    bricks_list_to_remove = form_bricks_list_to_remove_brick(
        mnode, volname, subvol_num, replica_num, remove_from_hot_tier,
        **kwargs)

    if not bricks_list_to_remove:
        g.log.error("Failed to form bricks list to remove-brick. "
                    "Hence unable to shrink volume %s", volname)
        return False

    if replica_num is not None or 'replica_count' in kwargs:
        if 'replica_count' in kwargs:
            replica_count = int(kwargs['replica_count'])
        if replica_num is not None:
            if isinstance(replica_num, int):
                replica_count = 1
            else:
                replica_count = len(replica_num)

        # Get replica count info.
        replica_count_info = get_replica_count(mnode, volname)

        if is_tiered_volume(mnode, volname):
            if remove_from_hot_tier:
                current_replica_count = (
                    int(replica_count_info['hot_tier_replica_count']))
            else:
                current_replica_count = (
                    int(replica_count_info['cold_tier_replica_count']))
        else:
            current_replica_count = (
                int(replica_count_info['volume_replica_count']))

        kwargs['replica_count'] = current_replica_count - replica_count

        if subvol_num is not None or 'distribute_count' in kwargs:
            force = False
        else:
            force = True

    # If force, then remove-bricks with force option
    if force:
        g.log.info("Removing bricks %s from volume %s with force option",
                   bricks_list_to_remove, volname)
        ret, _, _ = remove_brick(mnode, volname, bricks_list_to_remove,
                                 option="force", **kwargs)
        if ret != 0:
            g.log.error("Failed to remove bricks %s from the volume %s with "
                        "force option", bricks_list_to_remove, volname)
            return False
        g.log.info("Successfully removed bricks %s from the volume %s with "
                   "force option", bricks_list_to_remove, volname)
        return True

    # remove-brick start
    g.log.info("Start Removing bricks %s from the volume %s",
               bricks_list_to_remove, volname)
    ret, _, _ = remove_brick(mnode, volname, bricks_list_to_remove,
                             option="start", **kwargs)
    if ret != 0:
        g.log.error("Failed to start remove-brick of bricks %s on the volume "
                    "%s", bricks_list_to_remove, volname)
        return False
    g.log.info("Successfully started removal of bricks %s from the volume %s",
               bricks_list_to_remove, volname)

    # remove-brick status
    g.log.info("Logging remove-brick status of bricks %s on the volume %s",
               bricks_list_to_remove, volname)
    _, _, _ = remove_brick(mnode, volname, bricks_list_to_remove,
                           option="status", **kwargs)

    # Wait for rebalance started by remove-brick to complete
    _rc = False
    while rebalance_timeout > 0:
        ret, out, _ = remove_brick(mnode, volname, bricks_list_to_remove,
                                   option="status", xml=True, **kwargs)
        if ret != 0:
            g.log.error("Failed to get xml output remove-brick status of "
                        "bricks %s on volume %s", bricks_list_to_remove,
                        volname)
            return False

        try:
            root = etree.XML(out)
        except etree.ParseError:
            g.log.error("Failed to parse the gluster remove-brick status "
                        "xml output.")
            return False
        remove_brick_aggregate_status = {}
        for info in root.findall("volRemoveBrick"):
            for element in info.getchildren():
                if element.tag == "aggregate":
                    for elmt in element.getchildren():
                        remove_brick_aggregate_status[elmt.tag] = elmt.text
        if "completed" in remove_brick_aggregate_status['statusStr']:
            _rc = True
            break

        elif "in progress" in remove_brick_aggregate_status['statusStr']:
            rebalance_timeout = rebalance_timeout - 30
            time.sleep(30)
            continue
        else:
            g.log.error("Invalid status string in remove brick status")
            return False

    if not _rc:
        g.log.error("Rebalance started by remove-brick is not yet complete "
                    "on the volume %s", volname)
        return False
    g.log.info("Rebalance started by remove-brick is successfully complete "
               "on the volume %s", volname)

    # remove-brick status after rebalance is complete
    g.log.info("Checking remove-brick status of bricks %s on the volume %s "
               "after rebalance is complete", bricks_list_to_remove, volname)
    ret, _, _ = remove_brick(mnode, volname, bricks_list_to_remove,
                             option="status", **kwargs)
    if ret != 0:
        g.log.error("Failed to get status of remove-brick of bricks %s on "
                    "volume %s after rebalance is complete",
                    bricks_list_to_remove, volname)
    g.log.info("Successfully got remove-brick status of bricks %s on "
               "volume %s after rebalance is complete", bricks_list_to_remove,
               volname)

    # Commit remove-brick
    g.log.info("Commit remove-brick of bricks %s on volume %s",
               bricks_list_to_remove, volname)
    ret, _, _ = remove_brick(mnode, volname, bricks_list_to_remove,
                             option="commit", **kwargs)
    if ret != 0:
        g.log.error("Failed to commit remove-brick of bricks %s on volume %s",
                    bricks_list_to_remove, volname)
        return False
    g.log.info("Successfully commited remove-bricks of bricks %s on volume %s",
               bricks_list_to_remove, volname)

    # Delete the removed bricks
    if delete_bricks:
        for brick in bricks_list_to_remove:
            brick_node, brick_path = brick.split(":")
            _, _, _ = g.run(brick_node, "rm -rf %s" % brick_path)

    return True


def form_bricks_to_replace_brick(mnode, volname, servers, all_servers_info,
                                 src_brick=None, dst_brick=None,
                                 replace_brick_from_hot_tier=False):
    """Get src_brick, dst_brick to replace brick

    Args:
        mnode (str): Node on which commands has to be executed
        volname (str): volume name
        servers (list): List of servers in the storage pool.
        all_servers_info (dict): Information about all servers.
        example :
            all_servers_info = {
                'abc.lab.eng.xyz.com': {
                    'host': 'abc.lab.eng.xyz.com',
                    'brick_root': '/bricks',
                    'devices': ['/dev/vdb', '/dev/vdc', '/dev/vdd', '/dev/vde']
                    },
                'def.lab.eng.xyz.com':{
                    'host': 'def.lab.eng.xyz.com',
                    'brick_root': '/bricks',
                    'devices': ['/dev/vdb', '/dev/vdc', '/dev/vdd', '/dev/vde']
                    }
                }

    Kwargs:
        src_brick (str): Faulty brick which needs to be replaced

        dst_brick (str): New brick to replace the faulty brick

        replace_brick_from_hot_tier (bool): True If brick are to be
            replaced from hot_tier. False otherwise. Defaults to False.

    Returns:
        Tuple: (src_brick, dst_brick)
        Nonetype: if volume doesn't exists or any other failure.
    """
    # Check if volume exists
    if not volume_exists(mnode, volname):
        g.log.error("Volume %s doesn't exists.", volname)
        return None

    # Get Subvols
    subvols_info = get_subvols(mnode, volname)

    if not dst_brick:
        dst_brick = form_bricks_list(mnode=mnode, volname=volname,
                                     number_of_bricks=1,
                                     servers=servers,
                                     servers_info=all_servers_info)
        if not dst_brick:
            g.log.error("Failed to get a new brick to replace the faulty "
                        "brick")
            return None
        dst_brick = dst_brick[0]

    if not src_brick:
        # Randomly pick up a brick to bring the brick down and replace.
        if subvols_info['is_tier']:
            if replace_brick_from_hot_tier:
                subvols_list = subvols_info['hot_tier_subvols']
            else:
                subvols_list = subvols_info['cold_tier_subvols']
        else:
            subvols_list = subvols_info['volume_subvols']

        src_brick = (random.choice(random.choice(subvols_list)))

    return src_brick, dst_brick


def replace_brick_from_volume(mnode, volname, servers, all_servers_info,
                              src_brick=None, dst_brick=None,
                              delete_brick=True,
                              replace_brick_from_hot_tier=False):
    """Replace faulty brick from the volume.

    Args:
        mnode (str): Node on which commands has to be executed
        volname (str): volume name
        servers (str|list): A server|List of servers in the storage pool.
        all_servers_info (dict): Information about all servers.
        example :
            all_servers_info = {
                'abc.lab.eng.xyz.com': {
                    'host': 'abc.lab.eng.xyz.com',
                    'brick_root': '/bricks',
                    'devices': ['/dev/vdb', '/dev/vdc', '/dev/vdd', '/dev/vde']
                    },
                'def.lab.eng.xyz.com':{
                    'host': 'def.lab.eng.xyz.com',
                    'brick_root': '/bricks',
                    'devices': ['/dev/vdb', '/dev/vdc', '/dev/vdd', '/dev/vde']
                    }
                }

    Kwargs:
        src_brick (str): Faulty brick which needs to be replaced

        dst_brick (str): New brick to replace the faulty brick

        delete_bricks (bool): After remove-brick delete the removed bricks.

        replace_brick_from_hot_tier (bool): True If brick are to be
            replaced from hot_tier. False otherwise. Defaults to False.

    Returns:
        bool: True if replacing brick from the volume is successful.
            False otherwise.
    """
    if isinstance(servers, str):
        servers = [servers]

    # Check if volume exists
    if not volume_exists(mnode, volname):
        g.log.error("Volume %s doesn't exists.", volname)
        return False

    # Get Subvols
    subvols_info = get_subvols(mnode, volname)

    if not dst_brick:
        dst_brick = form_bricks_list(mnode=mnode, volname=volname,
                                     number_of_bricks=1,
                                     servers=servers,
                                     servers_info=all_servers_info)
        if not dst_brick:
            g.log.error("Failed to get a new brick to replace the faulty "
                        "brick")
            return False
        dst_brick = dst_brick[0]

    if not src_brick:
        # Randomly pick up a brick to bring the brick down and replace.
        if subvols_info['is_tier']:
            if replace_brick_from_hot_tier:
                subvols_list = subvols_info['hot_tier_subvols']
            else:
                subvols_list = subvols_info['cold_tier_subvols']
        else:
            subvols_list = subvols_info['volume_subvols']

        src_brick = (random.choice(random.choice(subvols_list)))

    # Brick the source brick offline
    from glustolibs.gluster.brick_libs import bring_bricks_offline
    g.log.info("Bringing brick %s offline of the volume  %s", src_brick,
               volname)
    ret = bring_bricks_offline(volname, src_brick)
    if not ret:
        g.log.error("Unable to bring brick %s offline for replace-brick "
                    "operation on volume %s", src_brick, volname)
        return False
    g.log.info("Successfully brought the brick %s offline for replace-brick "
               "operation on volume %s", src_brick, volname)

    # adding delay before performing replace-brick
    time.sleep(15)

    # Validate if the src_brick is offline
    from glustolibs.gluster.brick_libs import are_bricks_offline
    ret = are_bricks_offline(mnode, volname, [src_brick])
    if not ret:
        g.log.error("Brick %s is still not offline for replace-brick "
                    "operation on volume %s", src_brick, volname)
        return False
    g.log.info("Brick %s is offline for replace-brick operation", src_brick)

    # Log volume status before replace-brick
    g.log.info("Logging volume status before performing replace-brick")
    ret, _, _ = volume_status(mnode, volname)
    if ret != 0:
        g.log.error("Failed to get volume status before performing "
                    "replace-brick")
        return False

    # Replace brick
    g.log.info("Start replace-brick commit force of brick %s -> %s "
               "on the volume %s", src_brick, dst_brick, volname)
    ret, _, _ = replace_brick(mnode, volname, src_brick, dst_brick)
    if ret != 0:
        g.log.error("Failed to replace-brick commit force of brick %s -> %s "
                    "on the volume %s", src_brick, dst_brick, volname)
    g.log.info("Start replace-brick commit force of brick %s -> %s "
               "on the volume %s", src_brick, dst_brick, volname)

    # Delete the replaced brick
    if delete_brick:
        g.log.info("Deleting the replaced brick")
        brick_node, brick_path = src_brick.split(":")
        _, _, _ = g.run(brick_node, "rm -rf %s" % brick_path)

    return True


def get_client_quorum_info(mnode, volname):
    """Get the client quorum information. i.e the quorum type,
        quorum count.
    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.

    Returns:
        dict: client quorum information for the volume.
            client_quorum_dict = {
                'is_tier': False,
                'hot_tier_quorum_info':{
                    'is_quorum_applicable': False,
                    'quorum_type': None,
                    'quorum_count': None
                    },
                'cold_tier_quorum_info':{
                    'is_quorum_applicable': False,
                    'quorum_type': None,
                    'quorum_count': None
                    },
                'volume_quorum_info':{
                    'is_quorum_applicable': False,
                    'quorum_type': None,
                    'quorum_count': None
                    }
        }
        NoneType: None if volume doesnot exist.
    """
    client_quorum_dict = {
        'is_tier': False,
        'hot_tier_quorum_info': {
            'is_quorum_applicable': False,
            'quorum_type': None,
            'quorum_count': None
            },
        'cold_tier_quorum_info': {
            'is_quorum_applicable': False,
            'quorum_type': None,
            'quorum_count': None
            },
        'volume_quorum_info': {
            'is_quorum_applicable': False,
            'quorum_type': None,
            'quorum_count': None
            }
        }

    # Get quorum-type
    volume_option = get_volume_options(mnode, volname, 'cluster.quorum-type')
    if volume_option is None:
        g.log.error("Unable to get the volume option 'cluster.quorum-type' "
                    "for volume %s", volname)
        return client_quorum_dict
    quorum_type = volume_option['cluster.quorum-type']

    # Get quorum-count
    volume_option = get_volume_options(mnode, volname, 'cluster.quorum-count')
    if volume_option is None:
        g.log.error("Unable to get the volume option 'cluster.quorum-count' "
                    "for volume %s", volname)
        return client_quorum_dict
    quorum_count = volume_option['cluster.quorum-count']

    # Set the quorum info
    volume_type_info = get_volume_type_info(mnode, volname)
    if volume_type_info['is_tier'] is True:
        client_quorum_dict['is_tier'] = True

        # Hot Tier quorum info
        hot_tier_type = volume_type_info['hot_tier_type_info']['hotBrickType']
        if (hot_tier_type == 'Replicate' or
                hot_tier_type == 'Distributed-Replicate'):

            (client_quorum_dict['hot_tier_quorum_info']
             ['is_quorum_applicable']) = True
            replica_count = (volume_type_info['hot_tier_type_info']
                             ['hotreplicaCount'])

            # Case1: Replica 2
            if int(replica_count) == 2:
                if 'none' not in quorum_type:
                    (client_quorum_dict['hot_tier_quorum_info']
                     ['quorum_type']) = quorum_type

                    if quorum_type == 'fixed':
                        if not quorum_count == '(null)':
                            (client_quorum_dict['hot_tier_quorum_info']
                             ['quorum_count']) = quorum_count

            # Case2: Replica > 2
            if int(replica_count) > 2:
                if quorum_type == 'none':
                    (client_quorum_dict['hot_tier_quorum_info']
                     ['quorum_type']) = 'auto'
                    quorum_type == 'auto'
                else:
                    (client_quorum_dict['hot_tier_quorum_info']
                     ['quorum_type']) = quorum_type
                if quorum_type == 'fixed':
                    if not quorum_count == '(null)':
                        (client_quorum_dict['hot_tier_quorum_info']
                         ['quorum_count']) = quorum_count

        # Cold Tier quorum info
        cold_tier_type = (volume_type_info['cold_tier_type_info']
                          ['coldBrickType'])
        if (cold_tier_type == 'Replicate' or
                cold_tier_type == 'Distributed-Replicate'):
            (client_quorum_dict['cold_tier_quorum_info']
             ['is_quorum_applicable']) = True
            replica_count = (volume_type_info['cold_tier_type_info']
                             ['coldreplicaCount'])

            # Case1: Replica 2
            if int(replica_count) == 2:
                if 'none' not in quorum_type:
                    (client_quorum_dict['cold_tier_quorum_info']
                     ['quorum_type']) = quorum_type

                    if quorum_type == 'fixed':
                        if not quorum_count == '(null)':
                            (client_quorum_dict['cold_tier_quorum_info']
                             ['quorum_count']) = quorum_count

            # Case2: Replica > 2
            if int(replica_count) > 2:
                if quorum_type == 'none':
                    (client_quorum_dict['cold_tier_quorum_info']
                     ['quorum_type']) = 'auto'
                    quorum_type == 'auto'
                else:
                    (client_quorum_dict['cold_tier_quorum_info']
                     ['quorum_type']) = quorum_type
                if quorum_type == 'fixed':
                    if not quorum_count == '(null)':
                        (client_quorum_dict['cold_tier_quorum_info']
                         ['quorum_count']) = quorum_count
    else:
        volume_type = (volume_type_info['volume_type_info']['typeStr'])
        if (volume_type == 'Replicate' or
                volume_type == 'Distributed-Replicate'):
            (client_quorum_dict['volume_quorum_info']
             ['is_quorum_applicable']) = True
            replica_count = (volume_type_info['volume_type_info']
                             ['replicaCount'])

            # Case1: Replica 2
            if int(replica_count) == 2:
                if 'none' not in quorum_type:
                    (client_quorum_dict['volume_quorum_info']
                     ['quorum_type']) = quorum_type

                    if quorum_type == 'fixed':
                        if not quorum_count == '(null)':
                            (client_quorum_dict['volume_quorum_info']
                             ['quorum_count']) = quorum_count

            # Case2: Replica > 2
            if int(replica_count) > 2:
                if quorum_type == 'none':
                    (client_quorum_dict['volume_quorum_info']
                     ['quorum_type']) = 'auto'
                    quorum_type == 'auto'
                else:
                    (client_quorum_dict['volume_quorum_info']
                     ['quorum_type']) = quorum_type
                if quorum_type == 'fixed':
                    if not quorum_count == '(null)':
                        (client_quorum_dict['volume_quorum_info']
                         ['quorum_count']) = quorum_count

    return client_quorum_dict


def wait_for_volume_process_to_be_online(mnode, volname, timeout=300):
    """Waits for the volume's processes to be online until timeout

    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.

    Kwargs:
        timeout (int): timeout value in seconds to wait for all volume
        processes to be online.

    Returns:
        True if the volume's processes are online within timeout,
        False otherwise
    """
    # Adding import here to avoid cyclic imports
    from glustolibs.gluster.brick_libs import wait_for_bricks_to_be_online

    # Wait for bricks to be online
    bricks_online_status = wait_for_bricks_to_be_online(mnode, volname,
                                                        timeout)
    if bricks_online_status is False:
        g.log.error("Failed to wait for the volume '%s' processes "
                    "to be online", volname)
        return False

    # Wait for self-heal-daemons to be online
    self_heal_daemon_online_status = (
        wait_for_self_heal_daemons_to_be_online(mnode, volname, timeout))
    if self_heal_daemon_online_status is False:
        g.log.error("Failed to wait for the volume '%s' processes "
                    "to be online", volname)
        return False

    # TODO: Add any process checks here

    g.log.info("Volume '%s' processes are all online", volname)
    return True
