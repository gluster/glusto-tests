# get_sosreports
Tool to collect sosreports from all servers and clients.

## Prerequisites
1. Python 3.x
2. Passwordless ssh should be setup.

## Installation
1. Change directory to the project directory.

```
# cd tools/get_sosreports
```

2. Now run the installation script.

```
# python3 setup.py install
```

3. To check run:

```
# get_sosreports --help
```

## Usage
There are 2 ways of using the tool.
1. Passing IP addresses through command line seperated by comma(,):

```
# get_sosreports -m machine_1,machine_2,machine_3
```

2. Passing a glusto-tests config file:

```
# get_sosreports -f config_file
```

**Note**:
The default destination directory is `.` (present dir) `-d` or `--dist-dir` option.

## Licence
[GPLv3](https://github.com/gluster/glusto-tests/blob/master/LICENSE)
