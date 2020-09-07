# verify_test_execution
This tool verifies the stability of a given set of testcase(s) by executing it
consecutively for a pre-defined number of times. This ensures that the written
code is stable and also helps the user to identify unexpected failures or errors
that may arise while executing it multiple times. It also checks the given code
for any pylint/flake8 issues.

## Prerequisites
Python 3.x

To use this you need to have a valid glusto-tests config file

## Usage
- Download the project files from github.

  ```
     # git clone https://github.com/gluster/glusto-tests.git
  ```
- Change directory to the project directory.
  ```
     # cd glusto-tests/tool/verify_test_execution/
  ```
- To get help run:
  ```
     # python3 verify_test_execution.py --help
  ```
- To run the test(s):
  ```
    # python3 verify_test_execution.py --config < Config file> --test <test_path>
  ```

If you wish to specify the commands for flake8 and pylint (optional) use
`--flake8 <flake8 cmd> `and `--pylint <pylint command>` arguments.
Also, use `--iterations` to specify the number of times the test(s)
should be run (by default its 5) eg. `-- iterations 10 `

## License
GPLv3
