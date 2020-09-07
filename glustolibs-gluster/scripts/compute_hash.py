#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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

from __future__ import print_function
import ctypes
import sys

filename = sys.argv[1]
glusterfs = ctypes.cdll.LoadLibrary("libglusterfs.so.0")

# In case of python3 encode string to ascii
if sys.version_info.major == 3:
    computed_hash = ctypes.c_uint32(glusterfs.gf_dm_hashfn(
        filename.encode('ascii'), len(filename)))
else:
    computed_hash = ctypes.c_uint32(glusterfs.gf_dm_hashfn(
        filename, len(filename)))

print(computed_hash.value)
