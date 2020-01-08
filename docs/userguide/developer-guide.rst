#######################################
Developer Guide for Tests and Libraries
#######################################

This is a collection of best practices for test and library developers in this
repo. Almost all the items are requirements for code to be merged. When
something is a recommendation, it will be marked appropriately.

Follow the style guide
======================

Please follow the `PEP008 style guide`_.

- Line Length: Limit all lines to a maximum of 79 characters. When you break
  a line, use the tuple style::

    calling_a_function("This is the first line"
                       "This is the second line")

- Comments that contradict the code are worse than no comments. Always make
  a priority of keeping the comments up-to-date when the code changes. All
  comments in the code use the ``#`` format.

- Conventions for writing good documentation strings ("docstrings") are
  immortalized in `PEP 257`_. Only use the ``'''`` format for documentation
  strings. They're written at the start of a function or class.

- Please follow PEP8 guidelines for naming convenctions.

Best Practices
==============

- If you are not doing anything specific to your test in the ``setUp`` and
  ``tearDown`` class, please do not declare them only to call the base class'
  setUp and tearDown functions.

- Use the `assert functions`_ provided in the unittest library. Beyond
  ``assertTrue`` and ``assertEqual`` functions, there are also
  ``assertIn`` and ``assertRegexpMatches`` functions.

- Do **NOT** peer probe from your test or write code to check for peer probe
  status. The infrastructure setup will do the peer probe for you. If you use
  ``setup_volume`` from the ``GlusterBaseClass``, you are already checking
  if the hosts are peer probed. There's ``validate_peers_are_connected`` in
  ``GlusterBaseClass`` which can check peer probe status if you are not using
  ``setup_volume``. Exception: Your test is a glusterd test which actually
  tests peer probe functionality.

- Please write `meaningful commit messages`_.
    * The first line of the commit message should only have 72 characters and
        should stand on its own.
    * Leave a blank line and then have a detailed commit message.
    * When you update a patch, please do not edit the original commit message
      like would for Github. Changing it to "Fixed Review Comments" is a common
      mistake.

- Make sure the log messages are grammatically correct and have no spelling
  mistakes.

- Follow the Python `best practices`_ for writing tests.

- Comment every step of the test case, log the test step, test result, failure
  and success.

- In the ``setUp``, ``tearDown``, ``setUpClass``, and
  ``tearDownClass``, use Exceptions for unexpected results. In the test
  itself, only use asserts for testing outcome of what you're testing. For
  every other unexpected failure, raise an Exception.

::

    from glustolibs.gluster.gluster_base_class import GlusterBaseClass


    class GlusterTestClass(GlusterBaseClass):
        @classmethod
        def setUpClass(cls):
            cls.get_super_method(cls, 'setUpClass')()
            # Your code here
            # Remove this function if you don't have set up steps to do

        def setUp(self):
            self.get_super_method(self, 'setUp')()
            # Your code here
            # Remove this function if you don't have set up steps to do

        def test_name_of_test_case1(self):
            '''
            Docstring describing what this test will do
            '''
            pass

        def test_name_of_test_case2(self):
            '''
            Docstring describing what this test will do
            '''
            pass

        def tearDown(self):
             self.get_super_method(self, 'tearDown')()
            # Your code here
            # Remove this function if you don't have set up steps to do

        @classmethod
        def tearDownClass(cls):
            cls.get_super_method(cls, 'tearDownClass')()
            # Your code here
            # Remove this function if you don't have set up steps to do

Submitting patches for review
=============================

- Test the libraries and test case for all the volumes and mount protocols it
  can run on before submitting the patch.

- Check the test code against pep8 standards. We use flake8 for the lint on
  Gerrit. If you want to make sure it passes, run it locally before submitting
  a change.

- Add the owners of the component to review the patch. A review from the
  component owner is recommended but not required.

- Only mark the patch as Verified +1 if the test is verified across on all
  volume and mount protocols.

- If the patch contains library modifications, then ensure they are well
  tested and do not break current tests.

- Patches are merged by the Gluster component maintainers and peers.

.. note:: We recommend that the component owner reviews the any test which
    touches their component, however the lack of a review from them does not
    prevent a merge.

.. _PEP008 style guide: https://www.python.org/dev/peps/pep-0008/
.. _PEP257: https://www.python.org/dev/peps/pep-0257/
.. _assert functions: https://docs.python.org/2/library/unittest.html#unittest.TestCase.assertEqual
.. _best practices: https://docs.python.org/2/library/unittest.html#test-cases
