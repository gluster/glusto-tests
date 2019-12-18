# Glusto Tests

`glusto-tests` repo contains automated testcases for testing gluster software.
It provides the Libraries/Modules necessary for automating the gluster tests.
Latest Code for this repo is managed on review.gluster.org

The Libraries/Modules/Tests in glusto-tests are written using the `glusto`
framework. TestCases in glusto-tests can we written/run using standard
PyUnit, PyTest or Nose methodologies as supported by `glusto` framework.

Refer the [glusto-doc](http://glusto.readthedocs.io/en/latest/) for info on `glusto` framework.
Issues need to be filled against the [Github](https://github.com/gluster/glusto-tests/issues) repo.

To automate/run glusto-tests we need to install the following packages:
---------------------------------------------------------------------
-   glusto
-   glustolibs-gluster
-   glustolibs-io
-   glustolibs-misc
-   gdeploy

How to install glusto:
----------------------
One can use either of the two methods.

-   using pip

        # pip install --upgrade git+git://github.com/loadtheaccumulator/glusto.git

-   using git

        # git clone https://github.com/loadtheaccumulator/glusto.git
        # cd glusto
        # python setup.py install

For more info refer the [docs](http://glusto.readthedocs.io/en/latest/userguide/install.html).

How to install the glustolibs-gluster, glustolibs-io and glustolibs-misc libraries:
----------------------------------------------------------------------------------------------
    # git clone http://review.gluster.org/glusto-tests
    # cd glusto-tests/glustolibs-gluster
    # python setup.py install
    # cd ../../glusto-tests/glustolibs-io
    # python setup.py install
    # cd ../../glusto-tests/glustolibs-misc
    # python setup.py install

How to install gdeploy:
--------------------------------
-   Install latest version of gdeploy from the following [link](https://copr.fedorainfracloud.org/coprs/sac/gdeploy/package/gdeploy/).

To install glusto-tests dependencies:
--------------------------------------------------
- `python-docx`, `sh` and `numpy` has to be installed to run IO and validate it on client node.
    - To install run :

			    # curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
			    # python get-pip.py
			    # pip install --pre python-docx
			    # pip install numpy
			    # pip install sh

- `arequal` needs to be installed on all servers and clients.
	- To install download the below repo into /etc/yum.repos.d/

			# wget https://copr.fedorainfracloud.org/coprs/nigelbabu/arequal/repo/epel-7/nigelbabu-arequal-epel-7.repo
			# yum install arequal

Pre-requisites to run glusto-tests:
----------------------------------------------
- Make sure glusto, glusto-tests  are installed on the node from where you would want to run the gluster tests.
- Running Gluster Cluster( 6 Servers and 2 Clients )
- Gluster client packages should be installed on Clients
- Setup passwordless ssh from the glusto-tests management node to all.
- Install glusto-tests dependencies on servers and clients.
- Crefi should be installed on all the clients.

  ```
  $ pip install crefi
  $ pip install pyxattr
  ```

- Setup bricks on all servers:
  - To create bricks refer to [doc](https://gluster.readthedocs.io/en/latest/Administrator%20Guide/formatting-and-mounting-bricks/) **OR** Run gdeploy as shown below.
	1. Edit the `gdeploy_sample_config.conf` present in `examples` as shown below and also configure passwordless ssh to all servers:
    ```
    [hosts]
    server-vm1
    server-vm2
    server-vm3
    server-vm4
    server-vm5
    server-vm6

    [backend-setup]
    devices
    vgs
    pools
    lvs
    mountpoints
    ```
   	**Note:**
   	For more details you can view a sample config file avaliable at ``/usr/share/doc/gdeploy/examples/gluster.conf.sample`` which will be installed with gdeploy.

	2. Run gdeploy using the below command:
    ```
    gdeploy -c gdeploy_sample_config.conf
    ```
**Note:**

	- To run cifs protocol:
		1.CIFS packages need to be installed on the server
		2.Samba services need to be ACTIVE
		3.cifs-utils need to be installed on the client
	- To run nfs protocol, nfs packages must be installed on server and client

 For more info how to run glusto-tests from the scratch including creating OS, server, etc.. refer [link](https://github.com/gluster/glusto-tests/blob/master/docs/userguide/HOWTO)

How to run the test case:
----------------------------------
-  Update the information about the servers, clients, servers_info, client_info on the [config_file](https://github.com/gluster/glusto-tests/blob/master/tests/gluster_basic_config.yml), this information is enough to run all test cases. But if you need to override the default values of volumes, mount.. etc which is defined in gluster_base_class then use  [config](https://github.com/gluster/glusto-tests/blob/master/tests/gluster_tests_config.yml) and update the information accordingly.
Refer the following for more info [link](http://glusto.readthedocs.io/en/latest/userguide/configurable.html).

-   glusto-tests are run using the `glusto` command available after installing the glusto framework. The various options to run tests as provided by glusto framework: PyUnit Tests, PyTest Tests, Nose Tests.
The most common used is Pytest.
	- **Running PyTest Tests**
		- To run all tests that are marked with tag 'bvt':

				# glusto -c config.yml --pytest='-v -x tests -m bvt'
		- To run all tests that are under bvt folder:

				# glusto -c config.yml --pytest='-v -s bvt/'
		- To run a single test case:

				# glusto -c config.yml --pytest='-v -s -k test_demo1'

For more info about running tests on PyUnit, Pytest and Nose Tests, refer the [docs](http://glusto.readthedocs.io/en/latest/userguide/glusto.html#options-for-running-unit-tests).

glusto-tests can also be executed using `tox`:

```
       # tox -e functional -- glusto -c 'config.yml' --pytest='-v -s -k test_demo1'
```

glusto-tests can also be executed with python3 using `tox`:

```
       # tox -e functional3 -- glusto -c 'config.yml' --pytest='-v -s -k test_demo1'
```

**NOTE:**
- Please note that glusto-tests is not completely compatible with python3.
- You would not need to install the glusto or glusto-tests libraries while running it
  using `tox`. For more info about tox refer the [docs](https://tox.readthedocs.io/en/latest/#).

Writing tests in glusto-tests:
----------------------------------
- `tests` directory in glusto-tests contain testcases. Testcases are written as component wise.
Testcases name and file name should should start with **test_**.

- TestCases in glusto-tests can be written using standard PyUnit, PyTest or Nose methodologies as supported by `glusto` framework.
	- One can follow the [PyUnit](http://glusto.readthedocs.io/en/latest/userguide/unittest.html) docs to write PyUnit tests, or [PyTest](http://glusto.readthedocs.io/en/latest/userguide/pytest.html) docs to write PyTest tests, or [Nose](http://glusto.readthedocs.io/en/latest/userguide/nosetests.html) docs to write Nose tests.

**While writing testcases or libraries follow:**

- Please follow the [PEP008 style-guide](https://www.python.org/dev/peps/pep-0008/).
- Makes sure all the pylint and pyflakes error are fixed.
	For example

		- C0326: Exactly one space required around assignment
		- C0111: Missing module doc-string (missing-doc string)
		- W: 50: Too long line

	For more information on [pylint](https://docs.pylint.org/en/1.6.0/tutorial.html) and on [pyflakes](http://flake8.pycqa.org/en/latest/index.html).
	We can check for pyflakes and pylint errors:
	```
	# flake8 <test_script.py>
	or
	# flake8 <path_to_directory>
	# pylint -j 4 --rcfile=/glusto-tests/.pylintrc <test_script.py>
	```
- Optimize the code as much as possible. Eliminate the repetitive steps, write it has separate function.
- Use proper python standards on returning values. This style guide is a list of do's and don’ts for[ Python programs](http://google.github.io/styleguide/pyguide.html).
- Add docstring to every function you write
For example: This is an example of a module level function
  ```
	   def module(param1, param2):
       """
       Explain what the module function does in breif

       Args:
           param1: The first parameter.
           param2: The second parameter.

       Returns:
           The return value of the function.
       """
  ```

- Make sure the log messages are grammatically correct and have no spelling mistakes.

- Comment every step of the test case/libraries, log the test step, test result, failure and success.
For example:
   ```
   def test_peer_status(self):
       # peer status from mnode
       g.log.info("Get peer status from node %s", self.mnode)
       ret, out, err = peer_status(self.mnode)
       self.assertEqual(ret, 0, "Failed to get peer status from node %s: %s" % (self.mnode, err))
       g.log.info("Successfully got peer status from node %s:\n%s", self.mnode, out)
   ```


- Don't not use print statements in test-cases/libraries because prints statements are not captured in log files. Use logger functions to dump messages into log file.

For more info how to write testcases [developing-guide](https://github.com/gluster/glusto-tests/blob/master/docs/userguide/developer-guide.rst)

Logging:
--------------
Log file name and Log level can be passed as argument to glusto command while
running the glusto-tests. For example:

    # glusto -c 'config.yml' -l /tmp/glustotests_bvt.log --log-level DEBUG --pytest='-v -x tests -m bvt'

One can configure log files, log levels in the testcases as well. For details
on how to use glusto framework for configuring logs in tests Refer the following [docs](http://glusto.readthedocs.io/en/latest/userguide/loggable.html).

Default log location is: `/tmp/glustomain.log`

Note: When using `glusto` via the Python Interactive Interpreter,
the default log location is `/tmp/glusto.log`.

License
-------
[GPLv3](https://github.com/gluster/glusto-tests/blob/master/LICENSE)
