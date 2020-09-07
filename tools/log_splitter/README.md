# log_splitter
Tool to split glusto logs to individual testcase logs.

## Prerequisites
Python 3.x

## Installation
1. Change directory to the project directory.

```
# cd tools/log_splitter
```

2. Now run the installation script.

```
# python3 setup.py install
```

3. To check run:

```
# log_splitter --help
```

## Usage
Just pass glusto_test.log file to the script as shown below:

```
# log_splitter -f glusto_test.log
```

**Note**:
The default destination directory is `.` (present dir) `-d` or `--dist-dir` option.

## Licence
[GPLv3](https://github.com/gluster/glusto-tests/blob/master/LICENSE)
