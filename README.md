# Glusto Tests

`glusto-tests` repo contains automated testcases for testing gluster software.
It provides the Libraries/Modules necessary for automating the gluster tests.
Latest Code for this repo is managed on review.gluster.org

The Libraries/Modules/Tests in glusto-tests are written using the `glusto`
framework. TestCases in glusto-tests can we written/run using standard
PyUnit, PyTest or Nose methodologies as supported by `glusto` framework.

Refer the [glusto-doc](http://glusto.readthedocs.io/en/latest/) for info
on `glusto` framework.
Issues need to be filled against the
[Github](https://github.com/gluster/glusto-tests/issues) repo.

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
-----------------------------------------------------------------
    # git clone http://review.gluster.org/glusto-tests
    # cd glusto-tests/glustolibs-gluster
    # python setup.py install
    # cd ../../glusto-tests/glustolibs-io
    # python setup.py install
    # cd ../../glusto-tests/glustolibs-misc
    # python setup.py install

How to install gdeploy:
-----------------------
-   Install latest version of gdeploy from the following [link](https://copr.fedorainfracloud.org/coprs/sac/gdeploy/package/gdeploy/).

To install glusto-tests dependencies:
-------------------------------------
`python-docx` needs to be installed when we run IO's and validates on client node.

To install run :
        # curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
        # python get-pip.py
        # pip install --pre python-docx

How to run the test case:
-------------------------
-   Create config file containing info about the servers, clients,
    for example refer [config](https://github.com/gluster/glusto-tests/blob/master/tests/gluster_basic_config.yml),
    this config file is enough to run all test cases. But if you need to override the default values of volumes,
    mount.. etc which is passed in gluster_base_class, can be passed in config
    file[config](https://github.com/gluster/glusto-tests/blob/master/tests/gluster_tests_config.yml).
    Refer the following for more info [link](http://glusto.readthedocs.io/en/latest/userguide/configurable.html).

-   glusto-tests are run using the `glusto` command available after installing
    the glusto framework. The various options to run tests as provided by
    glusto framework:

    To run PyUnit tests:
        # glusto -c 'config.yml' -d 'tests'
        # glusto -c 'config.yml unittest_list.yml' -u

    To run PyTest tests:
        # glusto -c 'config.yml' --pytest='-v -x tests -m bvt'

    To run Nose tests:
        # glusto -c 'config.yml' --nosetests='-v -w tests'

    For more info about running these tests, refer the [docs](http://glusto.readthedocs.io/en/latest/userguide/glusto.html#options-for-running-unit-tests).

-   To know how to run glusto-tests from the scratch including creating OS, server, etc.. refer [link](https://github.com/gluster/glusto-tests/blob/master/docs/userguide/HOWTO)

Writing tests in glusto-tests:
------------------------------
`tests` directory in glusto-tests contains testcases. One might want to create
a dir with feature name as the name of test directory under `tests` to add
new testcases. Testcases name should start with`test_`

TestCases in glusto-tests can we written using standard PyUnit, PyTest or Nose
methodologies as supported by `glusto` framework.

One can follow the [PyUnit](http://glusto.readthedocs.io/en/latest/userguide/unittest.html) docs to write PyUnit tests,
or [PyTest](http://glusto.readthedocs.io/en/latest/userguide/pytest.html) docs to write PyTest tests,
or [Nose](http://glusto.readthedocs.io/en/latest/userguide/nosetests.html) docs to write Nose tests.

For more info how to write testcases [developing-guide](https://github.com/gluster/glusto-tests/blob/master/docs/userguide/developer-guide.rst)

Logging:
--------
Log file name and Log level can be passed as argument to glusto command while
running the glusto-tests. For example:

    # glusto -c 'config.yml' -l /tmp/glustotests_bvt.log --log-level DEBUG --pytest='-v -x tests -m bvt'

One can configure log files, log levels in the testcases as well. For details
on how to use glusto framework for configuring logs in tests Refer the following [docs](http://glusto.readthedocs.io/en/latest/userguide/loggable.html).

Default log location is: `/tmp/glustomain.log`

Note: When using `glusto` via the Python Interactive Interpreter,
the default log location is `/tmp/glusto.log`.
