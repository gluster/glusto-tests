""" This Module demostrates how to use functions available in gluster_init
    module
"""
from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (
    GlusterBaseClass,
    runs_on,
)
from glustolibs.gluster.gluster_init import (
    is_glusterd_running,
    restart_glusterd,
    start_glusterd,
    stop_glusterd,
)


@runs_on([['distributed-replicated', 'replicated'],
          ['glusterfs', 'nfs']])
class DemoGlusterInitClass(GlusterBaseClass):
    """Demonstrating all the functions available in gluster_init module
    """
    @classmethod
    def setUpClass(cls):
        """Define it when you need to execute something else than super's
           setUpClass method.
        """
        # Read all the cluster config from the g.config and assign it to
        # class variables
        cls.get_super_method(cls, 'setUpClass')()

    def setUp(self):
        """setUp required for tests
        """
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Check if glusterd is running on all servers(expected: active)
        g.log.info("Check if glusterd is running on all servers %s"
                   "(expected: active)", self.servers)
        ret = is_glusterd_running(self.servers)
        if ret == 0:
            g.log.info("Glusterd is running on all servers %s", self.servers)
        elif ret == 1:
            g.log.info("Glusterd is not running on all the servers %s",
                       self.servers)
        elif ret == -1:
            g.log.info("Glusterd is not running on all the servers %s. "
                       "PID is alive", self.servers)

    def test_glusterd_services(self):
        """Test restart, stop, start of glusterd
        """
        # restart glusterd on all servers
        g.log.info("Restart glusterd on all servers %s", self.servers)
        ret = restart_glusterd(self.servers)
        self.assertTrue(ret, ("Failed to restart glusterd on all servers %s",
                              self.servers))
        g.log.info("Successfully restarted glusterd on all servers %s",
                   self.servers)

        # Check if glusterd is running on all servers(expected: active)
        g.log.info("Check if glusterd is running on all servers %s"
                   "(expected: active)", self.servers)
        ret = is_glusterd_running(self.servers)
        self.assertEqual(ret, 0, ("Glusterd is not running on all servers %s",
                                  self.servers))
        g.log.info("Glusterd is running on all the servers %s", self.servers)

        # Stop glusterd on all servers
        g.log.info("Stop glusterd on all servers %s", self.servers)
        ret = stop_glusterd(self.servers)
        self.assertTrue(ret, ("Failed to stop glusterd on all servers %s",
                              self.servers))
        g.log.info("Successfully stopped glusterd on all servers %s",
                   self.servers)

        # Check if glusterd is running on all servers(expected: not running)
        g.log.info("Check if glusterd is running on all servers %s"
                   "(expected: not running)", self.servers)
        ret = is_glusterd_running(self.servers)
        self.assertNotEqual(ret, 0, ("Glusterd is still running on some "
                                     "servers %s", self.servers))
        g.log.info("Glusterd not running on any servers %s as expected.",
                   self.servers)

        # Start glusterd on all servers
        g.log.info("Start glusterd on all servers %s", self.servers)
        ret = start_glusterd(self.servers)
        self.assertTrue(ret, ("Failed to start glusterd on all servers %s",
                              self.servers))
        g.log.info("Successfully started glusterd on all servers %s",
                   self.servers)

        # Check if glusterd is running on all servers(expected: active)
        g.log.info("Check if glusterd is running on all servers %s"
                   "(expected: active)", self.servers)
        ret = is_glusterd_running(self.servers)
        self.assertEqual(ret, 0, ("Glusterd is not running on all servers %s",
                                  self.servers))
        g.log.info("Glusterd is running on all the servers %s", self.servers)

    def tearDown(self):
        """restart glusterd on all servers during teardown
        """
        # restart glusterd on all servers
        g.log.info("Restart glusterd on all servers %s", self.servers)
        ret = restart_glusterd(self.servers)
        if not ret:
            raise ExecutionError("Failed to restart glusterd on all "
                                 "servers %s", self.servers)
        g.log.info("Successfully restarted glusterd on all servers %s",
                   self.servers)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    @classmethod
    def tearDownClass(cls):
        """Define it when you need to execute something else than super's
           tearDownClass method.
        """
        cls.get_super_method(cls, 'tearDownClass')()
