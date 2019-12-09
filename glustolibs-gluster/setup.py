#!/usr/bin/env python
# Copyright (c) 2016-2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#

from setuptools import setup, find_packages
from distutils import dir_util

version = '0.22'
name = 'glustolibs-gluster'

setup(
    name=name,
    version=version,
    description='Glusto - Red Hat Gluster Libraries',
    license='GPLv3+',
    author='Red Hat, Inc.',
    author_email='gluster-devel@gluster.org',
    url='http://www.gluster.org',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta'
        'Environment :: Console'
        'Intended Audience :: Developers'
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)'
        'Operating System :: POSIX :: Linux'
        'Programming Language :: Python'
        'Programming Language :: Python :: 2'
        'Programming Language :: Python :: 2.6'
        'Programming Language :: Python :: 2.7'
        'Topic :: Software Development :: Testing'
    ],
    install_requires=['glusto'],
    dependency_links=['http://github.com/loadtheaccumulator/glusto/tarball/master#egg=glusto'],
    namespace_packages = ['glustolibs']
)

try:
    dir_util.copy_tree('./gdeploy_configs', '/usr/share/glustolibs/gdeploy_configs')
except:
    pass
