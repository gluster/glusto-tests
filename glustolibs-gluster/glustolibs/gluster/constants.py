#!/usr/bin/env python
#  Copyright (C) 2018 Red Hat, Inc. <http://www.redhat.com>
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
"""Constants for DHT library functions"""

# FILE TYPES
FILETYPE_DIR = 1
FILETYPE_DIRS = 1
FILETYPE_FILE = 2
FILETYPE_FILES = 2
FILETYPE_LINK = 4
FILETYPE_LINKS = 4
FILETYPE_ALL = 255

# TEST TYPES
TEST_LAYOUT_IS_COMPLETE = 1
TEST_LAYOUT_IS_BALANCED = 2
TEST_FILE_EXISTS_ON_HASHED_BRICKS = 4
TEST_ALL = 255
