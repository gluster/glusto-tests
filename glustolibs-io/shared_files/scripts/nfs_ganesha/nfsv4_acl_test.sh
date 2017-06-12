#!/bin/sh
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
#
#  Author: Jiffin Tony Thottan

# set ONLY_CREATE_USERS_AND_GROUPS to a non-empty string to exit before testing
#ONLY_CREATE_USERS_AND_GROUPS=yes

# if anything goes wrong, exit
set -e

TESTDIR=${1}
if [ -z "${TESTDIR}" -o ! -d "${TESTDIR}" ]
then
	echo "Usage: ${0} <testdir>"
        echo ""
        echo "    <testdir>    existing directory to use for testing"
        echo ""
	exit 1
fi

# get the domain for this system, maybe get it from /etc/idmapd.conf instead?

error()
{
	echo "ERROR: ${@}" > /dev/stderr
}

clean_mkdir()
{
	local dir=${1}

	[ -d ${dir} ] && rm -rf ${dir}
	mkdir ${dir}
}

OK()
{
	local user=${1}
	shift
	local cmd="${@}"

	if ! su ${user} sh -c "${cmd}" > /dev/null
	then
		error "FAILED as ${user}: ${cmd}"
		return 1
        else
                echo "OK as ${user}: ${cmd}"
	fi

	return 0
}

FAIL()
{
	local user=${1}
	shift
	local cmd="${@}"

	if su ${user} sh -c "${cmd}" > /dev/null 2>&1
	then
		error "OK but should have FAILED as ${user}: ${cmd}"
		return 1
	else
		echo "OK(=FAILED) as ${user}: ${cmd}"
	fi

	return 0
}

# test for existing user, create if missing
# there is no need for home directories
# each used should have its own group
add_user()
{
	local username=${1}
        shift
        local userid=${1}
	getent passwd ${username} >/dev/null \
                || useradd --no-create-home --shell /bin/sh --user-group ${username} -u ${userid}
}


add_user testuser1 5600
add_user testuser2 5601
add_user testuser3 5602
add_user testuser4 5603
add_user testuser5 5604
add_user testuser6 5605

# test for existing group, create if missing, add the additional
add_group()
{
	local groupname=${1}
	shift
        local groupid=${1}
        shift
	local users=${@}
	local username=''

	# create the group, if missing
	getent group ${groupname} > /dev/null \
                || groupadd ${groupname} -g ${groupid}

	# add each user to the group
	for username in ${users}
	do
		usermod -a -G ${groupname} ${username}
	done
}

add_group devgrp 6600 testuser2 testuser3
add_group qegrp 6601 testuser4
add_group managergrp 6602 testuser6 testuser5

# only create users/groups, exit here
if [ -n "${ONLY_CREATE_USERS_AND_GROUPS}" ]
then
	echo "Users and groups created, exiting..."
	exit 0
fi

cd ${TESTDIR}

# create an STATUS file where all users/groups can write progress
[ -e STATUS ] && rm -f STATUS
OK root "/bin/echo 'Status of this test:' > STATUS"
OK root nfs4_setfacl -a A:g:devgrp:RW STATUS
OK root nfs4_setfacl -a A:g:qegrp:RW STATUS
OK root nfs4_setfacl -a A:g:managergrp:RW STATUS

for USER in testuser2 testuser3 testuser4 testuser5 testuser6
do
	OK ${USER} "/bin/echo '- ${USER} can write' > STATUS"
done

OK testuser1 "cat STATUS"
FAIL testuser1 "/bin/echo '- testuser1 should not be able to write' > STATUS"

# some notes that testuser2 owns, only testuser3 may read them
[ -e NOTES.testuser2 ] && rm -f NOTES.testuser2
# create the file and hand it over to testuser2
OK root touch NOTES.testuser2
OK root chown testuser2:testuser2 NOTES.testuser2
# these notes are secret
OK testuser2 chmod 0600 NOTES.testuser2
sleep 2
OK testuser2 nfs4_setfacl -a A::testuser3:R NOTES.testuser2
# testuser2 should be able to write his own notes
OK testuser2 "/bin/echo 'This is my secret with testuser3' > NOTES.testuser2"
OK testuser3 cat NOTES.testuser2
FAIL testuser4 cat NOTES.testuser2
FAIL testuser5 cat NOTES.testuser2
# actually, also allow qegrp people to read the notes after previous failure
OK testuser2 nfs4_setfacl -a A:g:qegrp:R NOTES.testuser2
OK testuser4 cat NOTES.testuser2
FAIL testuser5 cat NOTES.testuser2

# any developer should be able to create filed/dirs under the src directory
clean_mkdir src
OK root nfs4_setfacl -a A:gdf:devgrp:RWX src
OK root nfs4_setfacl -a A:gdf:OWNER@:RWX src
OK testuser2 "/bin/echo 'Please send patches' > src/CONTRIBUTING"
OK testuser3 "/bin/echo 'Thanks to all contributors:' > src/THANKS"
OK testuser3 "/bin/echo 'Jiffin' >> src/THANKS"

# the testuser1 user should not be able to read our propriatary source code
OK root nfs4_setfacl -a D::testuser1:RWX -R src
FAIL testuser1 "cat src/THANKS"

# qegrp members may read the source code, but not modify it
OK root nfs4_setfacl -a A:g:qegrp:RX -R src
OK testuser4 "cat src/THANKS"
FAIL testuser4 "/bin/echo 'Saurabh' >> src/THANKS"
FAIL testuser4 "rm src/THANKS"

# newly added files should inherit the permissions, qegrp can read them
OK testuser3 "/bin/echo 'ACLs' > src/TODO"

# managergrp should create a compass for their reporting employees
clean_mkdir compass
OK root nfs4_setfacl -a "A:g:managergrp:RWX" compass
# devgrp and qegrp should be able to list contents of the compass directory
OK root nfs4_setfacl -a "A:g:devgrp:X" compass
OK root nfs4_setfacl -a "A:g:qegrp:X" compass
OK root nfs4_setfacl -a "D:dfi:EVERYONE@:RWX" compass
OK root nfs4_setfacl -a "A:dfi:OWNER@:RWX" compass
OK root nfs4_setfacl -a "A:dgfi:managergrp:RX" compass
OK testuser6 "/bin/echo 'You should have started compass yesterday' > compass/testuser3"
OK testuser6 nfs4_setfacl -a "A::testuser3:RW" compass/testuser3
# testuser5 should also be able to read testuser3' compass
OK testuser5 "cat compass/testuser3"
# testuser2 should not be able to read testuser3' compass
FAIL testuser2 "cat compass/testuser3"

# each employee may only read/edit their own compass
OK testuser3 "/bin/echo 'I will start really soon now...' > compass/testuser3"
OK testuser3 "cat compass/testuser3"
