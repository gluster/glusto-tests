#!/usr/bin/env python
#  Copyright (C) 2019  Red Hat, Inc. <http://www.redhat.com>
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
    Description: Library for glusterfind operations.
"""

from glusto.core import Glusto as g


def gfind_create(mnode, volname, sessname, debug=False, resetsesstime=False,
                 force=False):
    """Creates a glusterfind session for the given volume.

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
        sessname (str): session name

    Kwargs:
        debug (bool): If this option is set to True, then
            the command will be run with debug mode. If this option is
            set to False, then the command will not be run with debug mode.
        resetsesstime (bool): If this option is set to True, then
            the session time will be forced to be reset to the current time
            and the next incremental will start from this time. If this option
            is set to False then the session time will not be reset.
        force (bool): If this option is set to True, then glusterfind
            create will get execute with force option. If it is set to False,
            then glusterfind create will get executed without force option.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        gfind_create("abc.com", testvol, testsession)
        >>> (0, 'Session testsession created with volume alpha\n', '')
    """

    params = ''
    if debug:
        params = params + ' --debug'

    if resetsesstime:
        params = params + ' --reset-session-time'

    if force:
        params = params + ' --force'

    cmd = "glusterfind create %s %s %s" % (sessname, volname, params)
    return g.run(mnode, cmd)


def gfind_delete(mnode, volname, sessname, debug=False):
    """Deletes the given session

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
        sessname (str): session name

    Kwargs:
        debug (bool): If this option is set to True, then
            the command will be run with debug mode. If this option is
            set to False, then the command will not be run with debug mode.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        gfind_delete("abc.com", testvol, testsession)
    """

    params = ''
    if debug:
        params = params + ' --debug'

    cmd = "glusterfind delete %s %s %s" % (sessname, volname, params)
    return g.run(mnode, cmd)


def gfind_list(mnode, volname=None, sessname=None, debug=False):
    """Lists the sessions created

    Args:
        mnode (str): Node on which cmd has to be executed.

    Kwargs:
        volname (str): volume name. If this option is provided then
            the command will be run with the '--volume volname' option.
        sessname (str): session name. If this option is provided then
            the command will be run with the '--session sessname' option.
        debug (bool): If this option is set to True, then
            the command will be run with debug mode. If this option is
            set to False, then the command will not be run with debug mode.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        gfind_list("abc.com", testvol, testsession)
    """

    params = ''

    if not volname:
        volname = ''

    if volname:
        params = params + (" --volume %s" % volname)

    if not sessname:
        sessname = ''

    if sessname:
        params = params + (" --session %s" % sessname)

    if debug:
        params = params + ' --debug'

    cmd = "glusterfind list %s" % params
    return g.run(mnode, cmd)


def gfind_pre(mnode, volname, sessname, outfile='', **kwargs):
    """Retrieve the modified files and directories and store it in the outfile.

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
        sessname (str): session name
        outfile (str): This is the incremental list of modified files.

    Kwargs:

        **kwargs
            The keys, values in kwargs are:
                - full: (bool)|False
                - tagforfullfind: (str)|None
                - gftype: (str)|None
                - outprefix: (str)|None
                - fieldsep: (str)|None
                - debug: (bool)|False
                - noencode: (bool)|False
                - disablepartial: (bool)|False
                - namespace: (bool)|False
                - regenoutfile: (bool)|False

        Where:
        full (bool): If this option is set to True, then the command will be
            run with '--full' option and a full find will be performed.
            If this option is set to False, then the command will be run
            without the '--full' option.
        tagforfullfind (str): When running the command with '--full' option,
            a subset of files can be retrieved according to a tag.
        gftype (str): 'Type' option specifies the finding the list of files
            or directories only. If the value is set to 'f' then only the file
            types will be listed. If the value is set to 'd' then only the
            directory types will be listed. If the value is set to 'both' then
            the files and directories both will be listed.
        outprefix (str): Prefix to the path/name specified in the outfile.
        fieldsep (str): field-separator specifies the character/s that
            glusterfind output uses to separate fields
        debug (bool): If this option is set to True, then
            the command will be run with debug mode. If this option is
            set to False, then the command will not be run with debug mode.
        noencode (bool): If this option is set to True, then it disables
            encoding of file paths. If this option is set to False, then the
            command will run without --no-encode option.
        disablepartial (bool): If this option is set to True, then the
            partial-find feature will be disabled. If this option is set to
            False, then the default value will be respected.
        namespace (bool): If this option is set to True, then the command
            will be run with '--N' option and only namespace changes will
            be listed. If this option is set to False, then the command will
            be run without the '--N' option.
        regenoutfile (bool): If this option is set to True, then the outfile
            will be regenerated. If this option is set to False, then the
            outfile will not be regenerated.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

            (-1, None, None): If an invalid option is used in the command.

    Example:
        gfind_pre("abc.com", testvol, testsession, outfile=/newoutfile.txt)
    """

    outprefix = fieldsep = tagforfullfind = gftype = None
    full = debug = noencode = disablepartial = regenoutfile = namespace = False
    params = ''

    if 'outprefix' in kwargs:
        outprefix = str(kwargs['outprefix'])

    if 'fieldsep' in kwargs:
        fieldsep = str(kwargs['fieldsep'])

    if 'full' in kwargs:
        full = bool(kwargs['full'])

    if 'tagforfullfind' in kwargs:
        tagforfullfind = str(kwargs['tagforfullfind'])

    if 'gftype' in kwargs:
        gftype = str(kwargs['gftype'])

    if 'debug' in kwargs:
        debug = bool(kwargs['debug'])

    if 'noencode' in kwargs:
        noencode = bool(kwargs['noencode'])

    if 'disablepartial' in kwargs:
        disablepartial = bool(kwargs['disablepartial'])

    if 'regenoutfile' in kwargs:
        regenoutfile = bool(kwargs['regenoutfile'])

    if 'namespace' in kwargs:
        namespace = bool(kwargs['namespace'])

    if outfile == '':
        g.log.error("Invalid command: Outfile needs to be provided in order"
                    " for the pre command to run")
        return (-1, None, None)

    if outfile != '':
        params = params + (" %s" % outfile)

    if outprefix:
        params = params + (" --output-prefix %s" % outprefix)

    if fieldsep:
        params = params + (" --field-separator '%s'" % fieldsep)

    if not full and gftype:
        if gftype == 'both':
            params = params + ' --type both'
        else:
            g.log.error("Invalid command: The '--type' option with 'f' or "
                        "'d' as values can only be used along with "
                        "'--full' option")
            return (-1, None, None)

    if not gftype:
        gftype = ''

    if full:
        params = params + ' --full'

        gftypelist = ['f', 'd', 'both', '']
        if gftype in gftypelist:
            if gftype != '':
                params = params + (" --type %s" % gftype)
        else:
            g.log.error("Invalid value for the '--type' option of the "
                        "glusterfind pre command. Choose among 'f/d/both'.")
            return (-1, None, None)

        if tagforfullfind:
            params = params + (" --tag-for-full-find %s" % tagforfullfind)

    if debug:
        params = params + ' --debug'

    if noencode:
        params = params + ' --no-encode'

    if disablepartial:
        params = params + ' --disable-partial'

    if regenoutfile:
        params = params + ' --regenerate-outfile'

    if namespace:
        params = params + ' -N'

    cmd = "glusterfind pre %s %s %s" % (sessname, volname, params)
    return g.run(mnode, cmd)


def gfind_post(mnode, volname, sessname, debug=False):
    """Run to update the session time

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
        sessname (str): session name

    Kwargs:
        debug (bool): If this option is set to True, then
            the command will be run with debug mode. If this option is
            set to False, then the command will not be run with debug mode.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        gfind_post("abc.com", testvol, testsession)
    """

    params = ''
    if debug:
        params = params + ' --debug'

    cmd = "glusterfind post %s %s %s" % (sessname, volname, params)
    return g.run(mnode, cmd)


def gfind_query(mnode, volname, outfile='', since='', end='', **kwargs):
    """Get a list of changed files based on a specific timestamp.

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
        outfile (str): This is the incremental list of modified files.

    Kwargs:
        since (int): Timestamp from which the files need to be retrieved.
        end (int): Timestamp until which the files need to be retrieved.

        **kwargs:
            The keys, values in kwargs are:
                - full: (bool)|False
                - tagforfullfind: (str)|None
                - gftype: (str)|None
                - outprefix: (str)|None
                - fieldsep: (str)|None
                - debug: (bool)|False
                - noencode: (bool)|False
                - disablepartial: (bool)|False
                - namespace: (bool)|False

        Where:
        full (bool): If this option is set to True, then the command will be
            run with '--full' option and a full find will be performed.
            If this option is set to False, then the command will be run
            without the '--full' option.
        tagforfullfind (str): When running the command with '--full' option,
            a subset of files can be retrieved according to a tag.
        gftype (str): 'Type' option specifies the finding the list of files
            or directories only. If the value is set to 'f' then only the file
            types will be listed. If the value is set to 'd' then only the
            directory types will be listed. If the value is set to 'both' then
            the files and directories both will be listed.
        outprefix (str): Prefix to the path/name specified in the outfile.
        fieldsep (str): field-separator specifies the character/s that
            glusterfind output uses to separate fields
        debug (bool): If this option is set to True, then
            the command will be run with debug mode. If this option is
            set to False, then the command will not be run with debug mode.
        noencode (bool): If this option is set to True, then it disables
            encoding of file paths. If this option is set to False, then the
            command will run without --no-encode option.
        disablepartial (bool): If this option is set to True, then the
            partial-find feature will be disabled. If this option is set to
            False, then the default value will be respected.
        namespace (bool): If this option is set to True, then the command
            will be run with '--N' option and only namespace changes will
            be listed. If this option is set to False, then the command will
            be run without the '--N' option.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

            (-1, None, None): If an invalid option is used in the command.

    Example1:
        gfind_query("abc.com", testvol, outfile=/newoutfile.txt,
                    since=timestamp1, end=timestamp2, full=False)
    Example2:
        gfind_query("abc.com", testvol, outfile=/newoutfile.txt, gftype='f')
            The above example will fail because the
            'full' option is not provided.
    """

    outprefix = fieldsep = tagforfullfind = gftype = None
    full = debug = noencode = disablepartial = namespace = False
    params = ''

    if 'outprefix' in kwargs:
        outprefix = str(kwargs['outprefix'])

    if 'fieldsep' in kwargs:
        fieldsep = str(kwargs['fieldsep'])

    if 'full' in kwargs:
        full = bool(kwargs['full'])

    if 'tagforfullfind' in kwargs:
        tagforfullfind = str(kwargs['tagforfullfind'])

    if 'gftype' in kwargs:
        gftype = str(kwargs['gftype'])

    if 'debug' in kwargs:
        debug = bool(kwargs['debug'])

    if 'noencode' in kwargs:
        noencode = bool(kwargs['noencode'])

    if 'disablepartial' in kwargs:
        disablepartial = bool(kwargs['disablepartial'])

    if 'namespace' in kwargs:
        namespace = bool(kwargs['namespace'])

    if full and since != "" and end != "":
        g.log.error("Invalid command: Glusterfind query accepts either full or"
                    " the since/end timestamps")
        return (-1, None, None)

    if outfile == '':
        g.log.error("Invalid command: Outfile needs to be provided in order"
                    " for the query command to run")
        return (-1, None, None)

    if outfile != '':
        params = params + (" %s" % outfile)

    if not full:
        if since != '':
            params = params + (" --since-time %s" % since)
        if end != '':
            params = params + (" --end-time %s" % end)
        if gftype:
            if gftype == 'both':
                params = params + ' --type both'
            else:
                g.log.error("Invalid command: The '--type' option with 'f' or "
                            "'d' as values can only be used along with "
                            "'--full' option")
                return (-1, None, None)

    if not gftype:
        gftype = ''

    if full:
        params = params + ' --full'

        gftypelist = ['f', 'd', 'both', '']
        if gftype in gftypelist:
            if gftype != '':
                params = params + (" --type %s" % gftype)
        else:
            g.log.error("Invalid value for the '--type' option of the "
                        "glusterfind query command. Choose among 'f/d/both'.")
            return (-1, None, None)

        if tagforfullfind:
            params = params + (" --tag-for-full-find %s" % tagforfullfind)

    if outprefix:
        params = params + (" --output-prefix %s" % outprefix)

    if fieldsep:
        params = params + (" --field-separator '%s'" % fieldsep)

    if debug:
        params = params + ' --debug'

    if noencode:
        params = params + ' --no-encode'

    if disablepartial:
        params = params + ' --disable-partial'

    if namespace:
        params = params + ' -N'

    cmd = "glusterfind query %s %s" % (volname, params)
    return g.run(mnode, cmd)
