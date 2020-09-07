#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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
import subprocess
import sys
from datetime import datetime


class TestVerify:

    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.config = None
        self.test_path = None
        self.test_run_count = None
        self.pre_run_check = False
        iter_value_default = 5
        flake8_default = "flake8"
        pylint_default = "pylint"
        # Set the arguments
        self.parser.add_argument('-c', '--config',
                                 help='Path to config file',
                                 required=True)
        self.parser.add_argument('-t', '--test',
                                 help='path to test file/folder',
                                 required=True)
        self.parser.add_argument('-f', '--flake8',
                                 default=flake8_default,
                                 help='command to invoke flake8 '
                                      '(by default <flake8 path_to_py_file>)')
        self.parser.add_argument('-p', '--pylint',
                                 default=pylint_default,
                                 help='command to invoke pylint '
                                      '(by default <pylint path_to_py_file>)')
        self.parser.add_argument('-i', '--iterations',
                                 type=int,
                                 default=iter_value_default,
                                 help='Iterations to runs the tests '
                                      '(by default its 5)')
        args = self.parser.parse_args()

        # Get config file path
        self.config = args.config

        # Get test file or folder
        self.test_path = args.test

        # Get the pylint command
        self.pylint_cmd = args.pylint

        # Get the falke8 command
        self.flake8_cmd = args.flake8

        # Get the iteration count
        self.test_run_count = args.iterations

        # Verify flake8
        self.verify_flake8()

        # Verify Pylint
        self.verify_pylint()

        # Verify test run for user defined number of times
        self.execute_tests()

    def verify_pylint(self):
        """
        Verifies the given file has pylint issues or not.
        In case the path given for the test to execute is a folder, the pylint
        command returns all the issues in all the files present in the folder.
        Verifies the return code of pylint.
        """
        print("o Pylint Verification:")
        result = subprocess.run([self.pylint_cmd, self.test_path],
                                stdout=subprocess.PIPE)
        if result.returncode != 0:
            self._print_error(result.stdout)
            print("\t Pylint validation failed")
            self.pre_run_check = False
        else:
            print("\t Pylint validation successful")
            self.pre_run_check = True

    def verify_flake8(self):
        """
        Verifies the given file for falke8 issues. Executes the flake8 command
        and verifies the return code.
        """
        print("o Flake8 Verification:")
        result = subprocess.run([self.flake8_cmd, self.test_path],
                                stdout=subprocess.PIPE)
        if result.returncode != 0:
            self._print_error(result.stdout)
            sys.exit("[ERROR]: Flake8 validation Failed")
        print("\t Flake8 validation successful")

    def execute_tests(self):
        """
        Runs the given test for user defined number of times.
        """
        start_time = datetime.now()
        if not self.pre_run_check:
            print("========= WARNING =========")
            decision = input("There were some errors in the pre-check for "
                             "the given code. It is advised to fix all those "
                             "issues and the start executing the tests. To "
                             "continue to test execution press Y. To exit "
                             "press any other key : ")
            if decision.lower() != "y":
                sys.exit("[ERROR]: Aborted by user")
        cmd = ("glusto -c '{config_path}' --pytest='-v -x {test_path}'"
               .format(config_path=self.config, test_path=self.test_path))
        print("\no Run Tests")
        print("\t ==>[ ", cmd, " ]")
        for counter in range(1, self.test_run_count+1):
            print("\t Iteration : %s" % counter)
            process = subprocess.Popen(cmd, shell=True,
                                       stdout=subprocess.PIPE)
            process.wait()
            if process.returncode != 0:
                self._print_error(process.stdout.read())
                sys.exit("[ERROR]: Test Execution Failed")
            print("\n\t\t Status : PASS")
        end_time = datetime.now()
        print("[INFO] : Test Execution succeeded")
        print("\t Test : {test_name}".format(test_name=self.test_path))
        print("\t Iterations : {iter}".format(iter=str(self.test_run_count)))
        print("\t Completed in {time}".format(time=str(end_time-start_time)))

    @staticmethod
    def _print_error(err):
        """
        Prints the error from the stdout
        """
        print("\t [Error] \n\t", "-" * 100)
        output = err.decode("utf-8").split("\n")
        for line in output:
            if line:
                print("\t", str(line))
        print("\t", "-" * 100)


if __name__ == "__main__":
    TestVerify()
