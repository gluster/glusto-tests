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

import argparse

from glusto.core import Glusto as g


def handle_configs(config_list):
    """Load user configuration files"""

    # load user specified configs
    if config_list:
        config_files = config_list.split()
        g.config = g.load_configs(config_files)
        return True

    return False


def parse_args():
    """Parse arguments with newer argparse module
        (adds built-in required parm)
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Create output file based on template and config files')
    parser.add_argument("-c", "--config",
                        help="Config file(s) to read.",
                        action="store", dest="config_list",
                        default=None)
    parser.add_argument("-t", "--template",
                        help="Template file to render",
                        action="store", dest="template_file",
                        default=None)
    parser.add_argument("-o", "--output",
                        help="Output file for rendered template",
                        action="store", dest="output_file",
                        default=None)
    return parser.parse_args()


def main():
    """Main function"""

    args = parse_args()

    if args.config_list:
        handle_configs(args.config_list)
        g.show_config(g.config)

    output_file = "rendered_template.txt"
    if args.output_file:
        output_file = args.output_file

    if args.template_file:
        g.render_template(args.template_file, g.config, output_file)


if __name__ == '__main__':
    main()
