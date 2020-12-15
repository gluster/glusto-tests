#!/usr/bin/python3
#  Copyright (C) 2020  Red Hat, Inc. <http://www.redhat.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY :or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from setuptools import setup

setup(
    name='generate_glusto_config',
    author='Red Hat, Inc.',
    author_email='gluster-devel@gluster.org',
    url='http://www.gluster.org',
    license='GPLv3+',
    description=("Tool to generate config file for executing glusto tests."),
    py_modules=['generate_glusto_config'],
    entry_points="""
    [console_scripts]
    generate_glusto_config = generate_glusto_config:main
    """
)
