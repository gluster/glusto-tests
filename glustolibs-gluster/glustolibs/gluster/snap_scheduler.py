#!/usr/bin/env python
#  Copyright (C) 2017-2018  Red Hat, Inc. <http://www.redhat.com>
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

from glusto.core import Glusto as g

"""
    This file contains the Snapshot Scheduler operations
    like initialise scheduler, enable scheduler, add jobs,
    edit jobs, delete jobs, list jobs and scheduler status.
"""


def scheduler_init(servers):
    """Initialises snapshot scheduler
     Args:
         servers(str): List of servers on which cmd has to be executed.

     Returns:
         True - Success
         False - Failure
     Example:
          scheduler_init("abc.com")
    """

    if not isinstance(servers, list):
        servers = [servers]

    cmd = "snap_scheduler.py init"
    for server in servers:
        ret, _, _ = g.run(server, cmd)
        if ret != 0:
            g.log.error("Unable to initialize scheduler on %s", server)
            return False
    return True


def scheduler_enable(mnode):
    """Enables snapshot scheduler on given node
     Args:
           mnode (str): Node on which cmd has to be executed.

     Returns:
           tuple: Tuple containing three elements (ret, out, err).
           The first element 'ret' is of type 'int' and is the return value
           of command execution.
           The second element 'out' is of type 'str' and is the stdout value
           of the command execution.
           The third element 'err' is of type 'str' and is the stderr value
           of the command execution.

     Example:
        scheduler_enable("abc.com")
    """

    cmd = "snap_scheduler.py enable"
    return g.run(mnode, cmd)


def scheduler_disable(mnode):
    """Disables snapshot scheduler on given node
    Args:
        servers (str): servers on which cmd has to be executed.

    Returns:
            tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.
            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.
            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        scheduler_disable("abc.com")
    """

    cmd = "snap_scheduler.py disable"
    return g.run(mnode, cmd)


def scheduler_add_jobs(mnode, jobname, scheduler, volname):
    """Add snapshot scheduler Jobs on given node
    Args:
        mnode (str): Node on which cmd has to be executed.
        jobname (str): scheduled Jobname
        scheduler (str): "* * * * *"
        * * * * *
        | | | | |
        | | | | +---- Day of the Week   (range: 1-7, 1 standing for Monday)
        | | | +------ Month of the Year (range: 1-12)
        | | +-------- Day of the Month  (range: 1-31)
        | +---------- Hour              (range: 0-23)
        +------------ Minute            (range: 0-59)

        volname (str): Volume name to schedule a job.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.
            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.
            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        scheduler_add_jobs("abc.com", "jobname", "*/10 * * * *", "volname")

        """
    cmd = ("snap_scheduler.py add \"%s\" \"%s\" %s"
           % (jobname, scheduler, volname))
    return g.run(mnode, cmd)


def scheduler_edit_jobs(mnode, jobname, scheduler, volname):
    """Edit the existing scheduled job

    Args:
        mnode (str): Node on which cmd has to be executed.
        jobname (str): scheduled Jobname
        scheduler (str): "* * * * *"
        * * * * *
        | | | | |
        | | | | +---- Day of the Week   (range: 1-7, 1 standing for Monday)
        | | | +------ Month of the Year (range: 1-12)
        | | +-------- Day of the Month  (range: 1-31)
        | +---------- Hour              (range: 0-23)
        +------------ Minute            (range: 0-59)

        volname (str): Volume name to schedule a job.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.
            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.
            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        scheduler_edit_jobs("abc.com", "Job1", "scheduler", "volname")
    """
    cmd = ("snap_scheduler.py edit \"%s\" \"%s\" %s"
           % (jobname, scheduler, volname))
    return g.run(mnode, cmd)


def scheduler_list(mnode):
    """Executes snapshot scheduler list command

    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.
            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.
            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        scheduler_list("abc.com")
    """
    cmd = "snap_scheduler.py list"
    return g.run(mnode, cmd)


def scheduler_status(mnode):
    """Executes snapshot scheduler status command

    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.
            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.
            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        scheduler_status("abc.xyz.com")
    """
    cmd = "snap_scheduler.py status"
    return g.run(mnode, cmd)


def scheduler_delete(mnode, jobname):
    """Deletes the already scheduled job

    Args:
        mnode (str): Node on which cmd has to be executed.
        jobname (str): scheduled Jobname

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.
            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.
            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        scheduler_delete("abc.xyz.com", "Job1")
    """
    cmd = "snap_scheduler.py delete %s" % jobname
    return g.run(mnode, cmd)
