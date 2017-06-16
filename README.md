# Glusto Tests

'glusto-tests' repo contains automated testcases for testing gluster software.
It provides the Libraries/Modules necessary for automating the gluster tests.

The Libraries/Modules/Tests in glusto-tests are written using the 'glusto'
framework. TestCases in glusto-tests can we written/run using standard
PyUnit, PyTest or Nose methodologies as supported by 'glusto' framework.

Refer to 'http://glusto.readthedocs.io/en/latest/' for info on 'glusto'
framework.

To automate/run glusto-tests we need to install the following packages:
----------------------------------------------------------------------
-   glusto
-   glustolibs-gluster
-   glustolibs-io
-   gdeploy

How to install glusto:
----------------------
-   pip install
        # pip install --upgrade git+git://github.com/loadtheaccumulator/glusto.git

                                       or

-   git clone
        # git clone https://github.com/loadtheaccumulator/glusto.git
        # cd glusto
        # python setup.py

Refer to: http://glusto.readthedocs.io/en/latest/userguide/install.html

How to install the glustolibs-gluster and glustolibs-io libraries:
-----------------------------------------------------------------
    # git clone http://review.gluster.org/glusto-tests
    # cd glusto-tests/glustolibs-gluster
    # python setup.py install
    # cd glusto-tests/glustolibs-io
    # python setup.py install

How to install gdeploy:
-----------------------
-   Install latest version of gdeploy from below link.
    https://copr.fedorainfracloud.org/coprs/sac/gdeploy/package/gdeploy/

How to run the test case:
-------------------------
-   Create config file containing info about the servers, clients, volumes,
    mounts. Please refer to example config file under tests directory in
    glusto-tests repo. The example config file is in yaml format and
    defines sections which provides info about the gluster cluster.
    We can use any 'glusto' framework supported formats for writing the
    config files.
    Refer : http://glusto.readthedocs.io/en/latest/userguide/configurable.html

-   glusto-tests are run using the 'glusto' command available after installing
    the glusto framework. The various options to run tests as provided by
    glusto framework:

    To run PyUnit tests:
        # glusto -c 'config.yml' -d 'tests'
        # glusto -c 'config.yml unittest_list.yml' -u

    To run PyTest tests:
        # glusto -c 'config.yml' --pytest='-v -x tests -m bvt'

    To run Nose tests:
        # glusto -c 'config.yml' --nosetests='-v -w tests'

    Refer: http://glusto.readthedocs.io/en/latest/userguide/glusto.html#options-for-running-unit-tests

Writing tests in glusto-tests:
------------------------------
'tests' directory in glusto-tests contains testcases. One might want to create
a dir with feature name as the name of test directory under 'tests' to add
new testcases.

TestCases in glusto-tests can we written using standard PyUnit, PyTest or Nose
methodologies as supported by 'glusto' framework.

To write PyUnit tests:
http://glusto.readthedocs.io/en/latest/userguide/unittest.html

To write PyTest tests:
http://glusto.readthedocs.io/en/latest/userguide/pytest.html

To write Nose tests:
http://glusto.readthedocs.io/en/latest/userguide/nosetests.html

Logging:
--------
Log file name and Log level can be passed as argument to glusto command while
running the glusto-tests. For example:

    # glusto -c 'config.yml' -l /tmp/glustotests_bvt.log --log-level DEBUG --pytest='-v -x tests -m bvt'

One can configure log files, log levels in the testcases as well. For details
on how to use glusto framework for configuring logs in tests Refer to:
http://glusto.readthedocs.io/en/latest/userguide/loggable.html

Default log location is '/tmp/glustomain.log'.

Note: When using 'glusto' via the Python Interactive Interpreter,
the default log location is '/tmp/glusto.log'.
