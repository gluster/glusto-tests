# setup_passwordless_ssh
This is a tool to setup passwordless ssh to all nodes. It takes a glusto-tests
config file and password as input.

## Prerequisites
1. Python 3.x
2. All the servers should have the same password.
3. Install sshpass on the control node.

```
# yum install sshpass
```

## Installation
Download the project files from github.

```
# git clone https://github.com/gluster/glusto-tests.git
```
Change directory to the project directory.

```
# cd glusto-tests/tool/setup_passwordless_ssh/
```
Now run the installation script.

```
# python3 setup.py install
```
To check run:

```
setup_passwordless_ssh --help
```

## Usage
To use this you need to have a valid glusto-tests config file([Sample file](https://github.com/gluster/glusto-tests/tree/master/tests/))
after which just run the tool as shown below:

```
# setup_passwordless_ssh -c <Config file> -p <Password>
```
If you wish to establish passwordless ssh for a non-root user use `-u` or
`--username` option followed by the username.

## License
[GPLv3](https://github.com/gluster/glusto-tests/blob/master/LICENSE)