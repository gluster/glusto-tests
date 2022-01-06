#  Copyright (C) 2017-2018 Red Hat, Inc. <http://www.redhat.com>
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
import re
import time
from glusto.core import Glusto as g
from glustolibs.gluster.volume_libs import is_volume_exported
from glustolibs.gluster.mount_ops import GlusterMount
from glustolibs.gluster.ctdb_ops import (
    edit_hook_script,
    enable_ctdb_cluster,
    create_nodes_file,
    create_public_address_file,
    start_ctdb_service,
    is_ctdb_status_healthy)
from glustolibs.gluster.volume_libs import (
    setup_volume,
    wait_for_volume_process_to_be_online)


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
               "smbusers: %s", str(list(smb_users_info.keys())))

    # Verify if volume is shared
    ret = is_volume_exported(mnode, volname, "smb")
    if not ret:
        g.log.info("Volume %s is not exported as 'cifs/smb' share", volname)
        return False
    g.log.info("Volume %s is exported as 'cifs/smb' share", volname)
    return True


def is_ctdb_service_running(mnode):
    """Check if ctdb services is running on node

    Args:
        mnode (str): Node on which ctdb service status has to be verified.

    Returns:
        bool: True if ctdb service is running. False otherwise.
    """
    g.log.info("Check if CTDB service is running on %s", mnode)
    ret, out, _ = g.run(mnode, "service ctdb status")
    if "Active: active (running)" in out:
        return True
    return False


def stop_ctdb_service(mnode):
    """Stop ctdb service on the specified node.

    Args:
        mnode (str): Node on which ctdb service has to be stopped.

    Returns:
        bool: True on successfully stopping ctdb service. False otherwise.
    """
    g.log.info("Stopping CTDB Service on %s", mnode)
    # Stop ctdb service
    ret, _, _ = g.run(mnode, "service ctdb stop")
    if ret != 0:
        g.log.error("Unable to stop the ctdb service")
        return False
    g.log.info("Successfully stopped ctdb service")
    return True


def start_ctdb_service_prime_node(mnode):
    """Start ctdb service on the specified node.

    Args:
        mnode (str): Node on which ctdb service has to be started

    Returns:
        bool: True on successfully starting ctdb service. False otherwise.
    """
    g.log.info("Starting CTDB Service on %s", mnode)

    # Start ctdb service
    ret, _, _ = g.run(mnode, "service ctdb start")
    if ret != 0:
        g.log.error("Unable to start the ctdb service")
        return False
    g.log.info("Successfully started ctdb service")

    return True


def start_nmb_service(mnode):
    """Start nmb service on the specified node.

    Args:
        mnode (str): Node on which nmb service has to be started

    Returns:
        bool: True on successfully starting nmb service. False otherwise.
    """
    g.log.info("Starting nmb Service on %s", mnode)

    # Start nmb service
    ret, _, _ = g.run(mnode, "service nmb start")
    if ret != 0:
        g.log.error("Unable to start the nmb service")
        return False
    g.log.info("Successfully started nmb service")

    return True


def is_nmb_service_running(mnode):
    """Check if nmb service is running on node

    Args:
        mnode (str): Node on which nmb service status has to be verified.

    Returns:
        bool: True if nmb service is running. False otherwise.
    """
    g.log.info("Check if nmb service is running on %s", mnode)
    ret, out, _ = g.run(mnode, "service nmb status")
    if "Active: active (running)" in out:
        return True
    return False


def start_winbind_service(mnode):
    """Start winbind service on the specified node.

    Args:
        mnode (str): Node on which winbind service has to be started

    Returns:
        bool: True on successfully starting winbind service. False otherwise.
    """
    g.log.info("Starting winbind Service on %s", mnode)

    # Start winbind service
    ret, _, _ = g.run(mnode, "service winbind start")
    if ret != 0:
        g.log.error("Unable to start the winbind service")
        return False
    g.log.info("Successfully started winbind service")
    return True


def is_winbind_service_running(mnode):
    """Check if winbind service is running on node

    Args:
        mnode (str): Node on which winbind service status has to be verified.

    Returns:
        bool: True if winbind service is running. False otherwise.
    """
    g.log.info("Check if winbind service is running on %s", mnode)
    ret, out, _ = g.run(mnode, "service winbind status")
    if "Active: active (running)" in out:
        return True
    return False


def setup_samba_ctdb_cluster(servers, primary_node,
                             ctdb_volname,
                             ctdb_nodes, ctdb_vips, ctdb_volume_config,
                             all_servers_info):
    """
    Create ctdb-samba cluster if doesn't exists

    Returns:
        bool: True if successfully setup samba else false
    """
    # Check if ctdb setup is up and running
    if is_ctdb_status_healthy(primary_node):
        g.log.info("ctdb setup already up skipping "
                   "ctdb setup creation")
        return True
    g.log.info("Proceeding with ctdb setup creation")
    for mnode in servers:
        ret = edit_hook_script(mnode, ctdb_volname)
        if not ret:
            return False
        ret = enable_ctdb_cluster(mnode)
        if not ret:
            return False
        ret = create_nodes_file(mnode, ctdb_nodes)
        if not ret:
            return False
        ret = create_public_address_file(mnode, ctdb_vips)
        if not ret:
            return False
    g.log.info("Setting up ctdb volume %s", ctdb_volname)
    ret = setup_volume(mnode=primary_node,
                       all_servers_info=all_servers_info,
                       volume_config=ctdb_volume_config)
    if not ret:
        g.log.error("Failed to setup ctdb volume %s", ctdb_volname)
        return False
    g.log.info("Successful in setting up volume %s", ctdb_volname)

    # Wait for volume processes to be online
    g.log.info("Wait for volume %s processes to be online",
               ctdb_volname)
    ret = wait_for_volume_process_to_be_online(mnode, ctdb_volname)
    if not ret:
        g.log.error("Failed to wait for volume %s processes to "
                    "be online", ctdb_volname)
        return False
    g.log.info("Successful in waiting for volume %s processes to be "
               "online", ctdb_volname)

    # start ctdb services
    ret = start_ctdb_service(servers)
    if not ret:
        return False

    ret = is_ctdb_status_healthy(primary_node)
    if not ret:
        g.log.error("CTDB setup creation failed - exiting")
        return False
    g.log.info("CTDB setup creation successfull")
    return True


def samba_ad(all_servers, netbios_name, domain_name, ad_admin_user,
             ad_admin_passwd, idmap_range=None):
    """Active Directory Integration

    Args:
        all_servers [list]: List of all servers where AD needs to be setup.
        netbios_name (str): Provide netbios name
        domain_name (str): Provide domain name
        ad_admin_user (str): Provide admin user
        ad_admin_passwd (str): Provide admin password
        idmap_range (str): Provide idmap range

    Returns:
        bool: True on successfully setting up AD. False otherwise.
    """
    g.log.info("Setting up AD Integration on %s", all_servers)
    mnode = all_servers[0]
    if netbios_name == '':
        g.log.error("netbios name is missing")
        return False
    # Validate netbios name
    if len(netbios_name) < 1 or len(netbios_name) > 15:
        g.log.error("The NetBIOS name must be 1 to 15 characters in length.")
        return False
    validate_netbios_name = re.compile(r"(^[A-Za-z\d_!@#$%^()\-'"
                                       r"{}\.~]{1,15}$)")
    isnetbiosname = validate_netbios_name.match(netbios_name)
    if isnetbiosname is None:
        g.log.error("The NetBIOS name entered is invalid.")
        return False

    if domain_name == '':
        g.log.error("domain name is missing")
        return False
    validate_domain_name = re.compile(r"^(?=.{1,253}$)(?!.*\.\..*)(?!\..*)"
                                      r"([a-zA-Z0-9-]{,63}\.)"
                                      r"{,127}[a-zA-Z0-9-]{1,63}$")
    isdomain = validate_domain_name.match(domain_name)
    if isdomain is None:
        g.log.error("The AD domain name string is invalid")
        return False
    # ad_workgroup should be in capital letters
    ad_workgroup = domain_name.split(".")[0].upper()

    if idmap_range is None:
        idmap_range = '1000000-1999999'
    else:
        try:
            idmap_range_start = int(idmap_range.split("-")[0])
            idmap_range_end = int(idmap_range.split("-")[1])
        except Exception as e:
            g.log.error("Invalid format.Use \'m-n\' for the range %s", str(e))
            return False
        if int(idmap_range_start) < 10000:
            g.log.error("Please select a starting value 10000 or above")
            return False
        # Maximum UIDs is 2^32
        elif int(idmap_range_end) > 4294967296:
            g.log.error("Please select an ending value 4294967296 or below")
            return False

    # Run the below in all servers
    for node in all_servers:
        smb_conf_file = "/etc/samba/smb.conf"
        add_netbios_name = r"sed -i '/^\[global\]/a netbios name = %s' %s"
        ret, _, err = g.run(node, add_netbios_name
                            % (netbios_name, smb_conf_file))
        if ret != 0:
            g.log.error("Failed to set netbios name parameters in smb.conf "
                        "file due to %s", str(err))
            return False
        add_realm = r"sed -i '/^\[global\]/a realm = %s' %s"
        ret, _, err = g.run(node, add_realm % (domain_name, smb_conf_file))
        if ret != 0:
            g.log.error("Failed to set realm parameters in smb.conf file "
                        "due to %s", str(err))
            return False
        add_idmap_range = (r"sed -i '/^\[global\]/a idmap config \* : "
                           "range = %s' %s")
        ret, _, err = g.run(node, add_idmap_range
                            % (idmap_range, smb_conf_file))
        if ret != 0:
            g.log.error("Failed to set idmap range parameters in smb.conf "
                        "file due to %s", str(err))
            return False
        add_idmap_bcknd = (r"sed -i '/^\[global\]/a idmap config \* : "
                           "backend = tdb' %s")
        ret, _, err = g.run(node, add_idmap_bcknd % smb_conf_file)
        if ret != 0:
            g.log.error("Failed to set idmap bcknd parameters in smb.conf "
                        "file due to %s", str(err))
            return False
        add_workgroup = ("sed -i '/^\\tworkgroup = "
                         "MYGROUP/c\\\tworkgroup = %s' %s")
        ret, _, err = g.run(node, add_workgroup
                            % (ad_workgroup, smb_conf_file))
        if ret != 0:
            g.log.error("Failed to set workgroup parameters in smb.conf file "
                        " due to %s", str(add_workgroup))
            return False
        add_security = "sed -i '/^\\tsecurity = user/c\\\tsecurity = ads' %s"
        ret, _, err = g.run(node, add_security % smb_conf_file)
        if ret != 0:
            g.log.error("Failed to set security parameters in smb.conf "
                        "file due to %s", str(err))
            return False

    # Verifying the Samba AD Configuration running testparm
    smb_ad_list = ["netbios name = "+netbios_name,
                   "workgroup = "+ad_workgroup,
                   "realm = " + domain_name.upper(), "security = ADS",
                   "idmap config * : backend = tdb", "idmap config * "
                   ": range = "+str(idmap_range)]
    testparm_cmd = "echo -e "+'"'+"\n"+'"'+" | testparm -v"
    ret, out, _ = g.run(node, testparm_cmd)
    if ret != 0:
        g.log.error("Testparm Command Failed to Execute")
    g.log.info("Testparm Command Execute Success")
    for smb_options in smb_ad_list:
        smb_options = smb_options.strip()
        if smb_options not in str(out).strip():
            g.log.info("Option %s entry present not in testparm" % smb_options)
            return False
    g.log.info("All required samba ad options set in smb.conf")

    if ad_admin_user == '':
        ad_admin_user = 'Administrator'

    # nsswitch Configuration
    # Run these in all servers
    for node in all_servers:
        winbind_passwd = ("sed -i '/^passwd:     files sss/cpasswd:     "
                          "files winbind' /etc/nsswitch.conf")
        ret, _, err = g.run(node, winbind_passwd)
        g.log.info("MASTER %s" % str(ret))
        if ret != 0:
            g.log.error("Failed to set winbind passwd  parameters in "
                        "nsswitch.conf file due to %s", str(err))
            return False
        winbind_group = ("sed -i '/^group:      files sss/cgroup:      "
                         "files winbind' /etc/nsswitch.conf")
        ret, _, err = g.run(node, winbind_group)
        if ret != 0:
            g.log.error("Failed to set winbind group parameters "
                        "in nsswitch.conf file due to %s", str(err))
            return False

        # Disable samba & winbind scripts
        samba_script = "/etc/ctdb/events.d/50.samba"
        winbind_script = "/etc/ctdb/events.d/49.winbind"
        ret, _, err = g.run(node, "chmod -x " + samba_script)
        if ret != 0:
            g.log.error("Failed to disable samba script as %s", str(err))
            return False
        ret, _, err = g.run(node, "chmod -x " + winbind_script)
        if ret != 0:
            g.log.error("Failed to disable winbind script as %s", str(err))
            return False
        # stop ctdb if already running
        ret = is_ctdb_service_running(node)
        if ret:
            ret = stop_ctdb_service(node)
            if not ret:
                return ret
        ret = start_ctdb_service_prime_node(node)
        ret = is_ctdb_service_running(node)
        if ret:
            ret = is_smb_service_running(node)
            if ret:
                g.log.error("Samba services still running even after "
                            "samba script is disable")
                return False
            ret = is_winbind_service_running(node)
            if ret:
                g.log.error("Winbind services still running even after "
                            "winbind script is disable")
                return False

    # Join Active Directory Domain
    # One node only
    net_join_cmd = "net ads join -U "
    success_out = ("Joined '" + netbios_name +
                   "' to dns domain '" + domain_name + "'")
    ret, out, err = g.run(mnode, net_join_cmd + ad_admin_user +
                          "%" + ad_admin_passwd)
    if success_out not in str(out).strip():
        g.log.error("net ads join failed %s", str(err))
        return False
    g.log.info("Net ads join success")

    # RUN THESE IN ALL NODES
    for node in all_servers:
        ret = start_nmb_service(node)
        ret = is_nmb_service_running(node)
        if not ret:
            g.log.error("Failed to start nmb service")
            return False
        ret, _, err = g.run(node, "chmod +x " + samba_script)
        if ret != 0:
            g.log.error("Failed to enable samba script as %s", str(err))
            return False
        ret, _, err = g.run(node, "chmod +x " + winbind_script)
        if ret != 0:
            g.log.error("Failed to enable winbind script as %s", str(err))
            return False
        ret = stop_ctdb_service(node)
        if not ret:
            return False
        ret = start_ctdb_service_prime_node(node)
        ret = is_ctdb_service_running(node)
        if ret:
            count = 0
            while count < 95:
                ret = is_smb_service_running(node)
                if ret:
                    break
                time.sleep(2)
                count += 1
            if not ret:
                g.log.error("Samba services not started running even "
                            "after samba "
                            "script is enabled")
                return False
            ret = start_winbind_service(node)
            ret = is_winbind_service_running(node)
            if not ret:
                g.log.error("Winbind services not running even after winbind "
                            "script is enabled")
                return False

    # Verify/Test Active Directory and Services
    ret, out, err = g.run(mnode, "net ads testjoin")
    if "Join is OK" not in str(out).strip():
        g.log.error("net ads join validation failed %s", str(err))
        return False
    # Verify if winbind is operating correctly by executing the following steps
    ret, out, err = g.run(mnode, "wbinfo -t")
    if "succeeded" not in str(out):
        g.log.error("wbinfo -t command failed, ad setup is not correct %s",
                    str(err))
        return False

    # Execute the following command to resolve the given name to a Windows SID
    sid_cmd = ("wbinfo --name-to-sid '" + ad_workgroup +
               "\\" + ad_admin_user + "'")
    ret, out, err = g.run(mnode, sid_cmd)
    if "-500 SID_USER" not in str(out):
        g.log.error("Failed to execute wbinfo --name-to-sid command %s",
                    str(err))
        return False
    sid = str(out).split('SID')[0].strip()

    # Execute the following command to verify authentication:
    wbinfo_auth_cmd = ("wbinfo -a '" + ad_workgroup + "\\" + ad_admin_user +
                       "%" + ad_admin_passwd + "'")
    ret, out, err = g.run(mnode, wbinfo_auth_cmd)
    if "password authentication succeeded" not in str(out).strip():
        g.log.error("winbind does nothave authentication to acess "
                    "ad server %s", str(err))
        return False

    # Execute the following command to verify if the id-mapping is
    # working properly
    idmap_range_start = str(idmap_range.split("-")[0])
    id_map_cmd = "wbinfo --sid-to-uid " + sid
    ret, out, err = g.run(mnode, id_map_cmd)
    if str(out).strip() != str(idmap_range_start):
        g.log.error("id mapping is not correct %s", str(err))
        return False
    # getent password validation
    getent_cmd = "getent passwd '" + ad_workgroup + "\\" + ad_admin_user + "'"
    getent_expected = "/home/" + ad_workgroup + "/" + ad_admin_user.lower()
    ret, out, err = g.run(mnode, getent_cmd)
    if getent_expected not in str(out).strip():
        g.log.error("winbind Name Service Switch failed %s", str(err))
        return False
    return True
