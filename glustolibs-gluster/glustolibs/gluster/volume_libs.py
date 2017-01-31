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
    Description: Module for gluster volume related helper functions.
"""


from glusto.core import Glusto as g
import time
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.volume_ops import (volume_create, volume_start,
                                           set_volume_options, get_volume_info,
                                           volume_stop, volume_delete,
                                           volume_info, volume_status)
from glustolibs.gluster.tiering_ops import (add_extra_servers_to_cluster,
                                            tier_attach,
                                            is_tier_process_running)
from glustolibs.gluster.quota_ops import (enable_quota, set_quota_limit_usage,
                                          is_quota_enabled)
from glustolibs.gluster.uss_ops import enable_uss, is_uss_enabled
from glustolibs.gluster.samba_ops import share_volume_over_smb
from glustolibs.gluster.snap_ops import snap_delete_by_volumename
from glustolibs.gluster.brick_libs import are_bricks_online, get_all_bricks
from glustolibs.gluster.heal_libs import are_all_self_heal_daemons_are_online


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
        g.log.error("Unable to create volume %s" % volname)
        return False

    # Start Volume
    time.sleep(2)
    ret = volume_start(mnode, volname)
    if not ret:
        g.log.error("volume start %s failed" % volname)
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
        rc = True
        for server in extra_servers:
            ret = is_tier_process_running(server, volname)
            if not ret:
                g.log.error("Tier process not running on %s", server)
                rc = False
        if not rc:
            return False

    # Enable Quota
    if ('quota' in volume_config and 'enable' in volume_config['quota'] and
            volume_config['quota']['enable']):
        ret, _, _ = enable_quota(mnode=mnode, volname=volname)
        if ret != 0:
            g.log.error("Unable to set quota on the volume %s", volname)
            return False

        # Check if 'limit_usage' is defined
        if ('limit_usage' in volume_config['quota']):
            if ('path' in volume_config['quota']['limit_usage']):
                path = volume_config['quota']['limit_usage']['path']
            else:
                path = "/"

            if ('size' in volume_config['quota']['limit_usage']):
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

    # Enable Ganesha
#    if ('nfs_ganesha' in volume_config and
#            'enable' in volume_config['nfs_ganesha'] and
#            volume_config['nfs_ganesha']['enable']):
#        from glustolibs.gluster.ganesha import vol_set_ganesha
#        ret = vol_set_ganesha(mnode=mnode, volname=volname, option=True)
#        if not ret:
#            g.log.error("failed to set the ganesha option for %s" % volname)
#            return False

    # Enable Samba
    if ('smb' in volume_config and 'enable' in volume_config['smb'] and
            volume_config['smb']['enable']):
        smb_users_info = {}
        if ('users_info' in volume_config['smb'] and
                volume_config['smb']['users_info']):
            smb_users_info = volume_config['smb']['users_info']
        else:
            g.log.error("SMB Users info not available in the volume config."
                        "Unable to export volume %s as SMB Share" % volname)
            return False
        ret = share_volume_over_smb(mnode=mnode, volname=volname,
                                    servers=servers,
                                    smb_users_info=smb_users_info)
        if not ret:
            g.log.error("Failed to export volume %s as SMB Share" % volname)
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
        g.log.info("Volume %s does not exist in %s" % (volname, mnode))
        return True

    ret, _, _ = snap_delete_by_volumename(mnode, volname)
    if ret != 0:
        g.log.error("Failed to delete the snapshots in "
                    "volume %s" % volname)
        return False

    ret, _, _ = volume_stop(mnode, volname, force=True)
    if ret != 0:
        g.log.error("Failed to stop volume %s" % volname)
        return False

    ret = volume_delete(mnode, volname)
    if not ret:
        g.log.error("Unable to cleanup the volume %s" % volname)
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

        cmd = "showmount -e localhost | grep %s" % volname
        ret, _, _ = g.run(mnode, cmd)
        if ret != 0:
            return False
        else:
            return True

    if 'cifs' in share_type:
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
        'hot_tier_subvols': [],
        'cold_tier_subvols': [],
        'volume_subvols': []
        }
    volinfo = get_volume_info(mnode, volname)
    if volinfo is not None:
        voltype = volinfo[volname]['typeStr']
        if voltype == 'Tier':
            # Get hot tier subvols
            hot_tier_type = (volinfo[volname]["bricks"]
                             ['hotBricks']['hotBrickType'])
            tmp = volinfo[volname]["bricks"]['hotBricks']["brick"]
            hot_tier_bricks = [x["name"] for x in tmp if "name" in x]
            if (hot_tier_type == 'Distribute'):
                for brick in hot_tier_bricks:
                    subvols['hot_tier_subvols'].append([brick])

            elif (hot_tier_type == 'Replicate' or
                  hot_tier_type == 'Distributed-Replicate'):
                rep_count = int((volinfo[volname]["bricks"]['hotBricks']
                                ['numberOfBricks']).split("=", 1)[0].
                                split("x")[1].strip())
                subvol_list = ([hot_tier_bricks[i:i + rep_count]
                               for i in range(0, len(hot_tier_bricks),
                                rep_count)])
                subvols['hot_tier_subvols'] = subvol_list

            # Get cold tier subvols
            cold_tier_type = (volinfo[volname]["bricks"]['coldBricks']
                              ['coldBrickType'])
            tmp = volinfo[volname]["bricks"]['coldBricks']["brick"]
            cold_tier_bricks = [x["name"] for x in tmp if "name" in x]

            # Distribute volume
            if (cold_tier_type == 'Distribute'):
                for brick in cold_tier_bricks:
                    subvols['cold_tier_subvols'].append([brick])

            # Replicate or Distribute-Replicate volume
            elif (cold_tier_type == 'Replicate' or
                  cold_tier_type == 'Distributed-Replicate'):
                rep_count = int((volinfo[volname]["bricks"]['coldBricks']
                                ['numberOfBricks']).split("=", 1)[0].
                                split("x")[1].strip())
                subvol_list = ([cold_tier_bricks[i:i + rep_count]
                               for i in range(0, len(cold_tier_bricks),
                                rep_count)])
                subvols['cold_tier_subvols'] = subvol_list

            # Disperse or Distribute-Disperse volume
            elif (cold_tier_type == 'Disperse' or
                  cold_tier_type == 'Distributed-Disperse'):
                disp_count = sum([int(nums) for nums in
                                 ((volinfo[volname]["bricks"]['coldBricks']
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

        elif (voltype == 'Disperse' or voltype == 'Distributed-Disperse'):
            disp_count = int(volinfo[volname]['disperseCount'])
            subvol_list = [bricks[i:i + disp_count]for i in range(0,
                                                                  len(bricks),
                                                                  disp_count)]
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
        g.log.error("Unable to get the volume info for volume %s" % volname)
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
        g.log.error("Unable to check if the volume %s is distribute" % volname)
        return False

    if volume_type_info['is_tier']:
        hot_tier_type = (volume_type_info['hot_tier_type_info']
                         ['hotBrickType'])
        cold_tier_type = (volume_type_info['cold_tier_type_info']
                          ['coldBrickType'])
        if (hot_tier_type == 'Distribute' and cold_tier_type == 'Distribute'):
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
                        'typeStr': 'Disperse'
                        'replicaCount': 1
                        'stripeCount': 1
                        'disperseCount': '3'
                        'redundancyCount': '1'
                        }
                    }

                volume_type_info = {
                    'is_tier': True,
                    'hot_tier_type_info': {
                        'hotBrickType': 'Distribute',
                        'hotreplicaCount': 1
                        },
                    'cold_tier_type_info': {
                        'coldBrickType': 'Disperse',
                        'coldreplicaCount': 1,
                        'colddisperseCount':3,
                        'numberOfBricks':1
                        },
                    'volume_type_info': {}


        NoneType: None if volume does not exist or any other key errors.
    """
    volinfo = get_volume_info(mnode, volname)
    if volinfo is None:
        g.log.error("Unable to get the volume info for volume %s" % volname)
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
            'stripeCount': '',
            'disperseCount': '',
            'redundancyCount': ''
            }
        for key in non_tiered_volume_type_info.keys():
            if key in volinfo[volname]:
                non_tiered_volume_type_info[key] = volinfo[volname][key]
            else:
                g.log.error("Unable to find key '%s' in the volume info for "
                            "the volume %s" % (key, volname))
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
                    'colddisperseCount': '3',
                    'numberOfBricks': '3'
                    }
        NoneType: None if volume does not exist or is not a tiered volume or
            any other key errors.
    """
    volinfo = get_volume_info(mnode, volname)
    if volinfo is None:
        g.log.error("Unable to get the volume info for volume %s" % volname)
        return None

    if not is_tiered_volume(mnode, volname):
        g.log.error("Volume %s is not a tiered volume" % volname)
        return None

    cold_tier_type_info = {
        'coldBrickType': '',
        'coldreplicaCount': '',
        'colddisperseCount': '',
        'numberOfBricks': ''
        }
    for key in cold_tier_type_info.keys():
        if key in volinfo[volname]['bricks']['coldBricks']:
            cold_tier_type_info[key] = (volinfo[volname]['bricks']
                                        ['coldBricks'][key])
        else:
            g.log.error("Unable to find key '%s' in the volume info for the "
                        "volume %s" % (key, volname))
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
        g.log.error("Unable to get the volume info for volume %s" % volname)
        return None

    if not is_tiered_volume(mnode, volname):
        g.log.error("Volume %s is not a tiered volume" % volname)
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
                        "volume %s" % (key, volname))
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
        g.log.error("Volume %s is not a tiered volume" % volname)
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
        g.log.error("Volume %s is not a tiered volume" % volname)
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
        g.log.error("Unable to get the replica count info for the volume %s" %
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
        g.log.error("Unable to get the disperse count info for the volume %s" %
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
