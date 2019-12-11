import calendar
import sys
import time

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           select_bricks_to_bring_offline,
                                           are_bricks_offline)
from glustolibs.gluster.heal_libs import is_shd_daemonized
from glustolibs.gluster.volume_ops import volume_start
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (validate_io_procs,
                                 wait_for_io_to_complete)


# pylint: disable=too-many-lines

@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed'], ['glusterfs', 'nfs']])
class SelfHealDaemonProcessTests(GlusterBaseClass):
    """
    SelfHealDaemonProcessTests contains tests which verifies the
    self-heal daemon process of the nodes
    """

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on mounts",
                   cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, [script_local_path])
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        """
        setup volume, mount volume and initialize necessary variables
        which is used in tests
        """

        # calling GlusterBaseClass setUpClass
        self.get_super_method(self, 'setUp')()

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        # Verfiy glustershd process releases its parent process
        ret = is_shd_daemonized(self.servers)
        if not ret:
            raise ExecutionError("Self Heal Daemon process was still"
                                 " holding parent process.")
        g.log.info("Self Heal Daemon processes are online")

        self.glustershd = "/var/lib/glusterd/glustershd/glustershd-server.vol"

    def tearDown(self):
        """
        Clean up the volume and umount volume from client
        """

        # Wait for IO to complete, if not completed
        if not self.io_validation_complete:
            g.log.info("Wait for IO to complete as IO validation did not "
                       "succeed in test method")
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")
            g.log.info("IO is successful on all mounts")

        # Unmount Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # calling GlusterBaseClass tearDownClass
        self.get_super_method(self, 'tearDown')()

    def test_brick_process_not_started_on_read_only_node_disks(self):
        """
        * create volume and start
        * kill one brick
        * start IO
        * unmount the brick directory from node
        * remount the brick directory with read-only option
        * start the volume with "force" option
        * check for error 'posix: initializing translator failed' in log file
        * remount the brick directory with read-write option
        * start the volume with "force" option
        * validate IO
        """
        # pylint: disable=too-many-locals,too-many-statements
        # Select bricks to bring offline
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = list(filter(None, (
            bricks_to_bring_offline_dict['hot_tier_bricks'] +
            bricks_to_bring_offline_dict['cold_tier_bricks'] +
            bricks_to_bring_offline_dict['volume_bricks'])))

        # Bring brick offline
        g.log.info('Bringing bricks %s offline...', bricks_to_bring_offline)
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        bricks_to_bring_offline)

        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % bricks_to_bring_offline)
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_to_bring_offline)

        # Creating files for all volumes
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            cmd = ("/usr/bin/env python%d %s create_files -f 100 "
                   "%s/%s/test_dir" % (
                       sys.version_info.major, self.script_upload_path,
                       mount_obj.mountpoint, mount_obj.client_system))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)

        # umount brick
        brick_node, volume_brick = bricks_to_bring_offline[0].split(':')
        node_brick = '/'.join(volume_brick.split('/')[0:3])
        g.log.info('Start umount brick %s...', node_brick)
        ret, _, _ = g.run(brick_node, 'umount -l %s' % node_brick)
        self.assertFalse(ret, 'Failed to umount brick %s' % node_brick)
        g.log.info('Successfully umounted %s', node_brick)

        # get time before remount the directory and checking logs for error
        g.log.info('Getting time before remount the directory and '
                   'checking logs for error...')
        _, time_before_checking_logs, _ = g.run(brick_node, 'date -u +%s')
        g.log.info('Time before remount the directory and checking logs - %s',
                   time_before_checking_logs)

        # remount the directory with read-only option
        g.log.info('Start remount brick %s with read-only option...',
                   node_brick)
        ret, _, _ = g.run(brick_node, 'mount -o ro %s' % node_brick)
        self.assertFalse(ret, 'Failed to remount brick %s' % node_brick)
        g.log.info('Successfully remounted %s with read-only option',
                   node_brick)

        # start volume with "force" option
        g.log.info('starting volume with "force" option...')
        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertFalse(ret, 'Failed to start volume %s with "force" option'
                         % self.volname)
        g.log.info('Successfully started volume %s with "force" option',
                   self.volname)

        # check logs for an 'initializing translator failed' error
        g.log.info("Checking logs for an 'initializing translator failed' "
                   "error for %s brick...", node_brick)
        error_msg = 'posix: initializing translator failed'
        cmd = ("cat /var/log/glusterfs/bricks/%s-%s-%s.log | "
               "grep '%s'"
               % (volume_brick.split('/')[-3], volume_brick.split('/')[-2],
                  volume_brick.split('/')[-1], error_msg))
        ret, log_msgs, _ = g.run(brick_node, cmd)
        log_msg = log_msgs.rstrip().split('\n')[-1]

        self.assertTrue(error_msg in log_msg, 'No errors in logs')
        g.log.info('EXPECTED: %s', error_msg)

        # get time from log message
        log_time_msg = log_msg.split('E')[0][1:-2].split('.')[0]
        log_time_msg_converted = calendar.timegm(time.strptime(
            log_time_msg, '%Y-%m-%d %H:%M:%S'))
        g.log.info('Time_msg from logs - %s ', log_time_msg)
        g.log.info('Time from logs - %s ', log_time_msg_converted)

        # get time after remount the directory checking logs for error
        g.log.info('Getting time after remount the directory and '
                   'checking logs for error...')
        _, time_after_checking_logs, _ = g.run(brick_node, 'date -u +%s')
        g.log.info('Time after remount the directory and checking logs - %s',
                   time_after_checking_logs)

        # check time periods
        g.log.info('Checking if an error is in right time period...')
        self.assertTrue(int(time_before_checking_logs) <=
                        int(log_time_msg_converted) <=
                        int(time_after_checking_logs),
                        'Expected error is not in right time period')
        g.log.info('Expected error is in right time period')

        # umount brick
        g.log.info('Start umount brick %s...', node_brick)
        ret, _, _ = g.run(brick_node, 'umount -l %s' % node_brick)
        self.assertFalse(ret, 'Failed to umount brick %s' % node_brick)
        g.log.info('Successfully umounted %s', node_brick)

        # remount the directory with read-write option
        g.log.info('Start remount brick %s with read-write option...',
                   node_brick)
        ret, _, _ = g.run(brick_node, 'mount %s' % node_brick)
        self.assertFalse(ret, 'Failed to remount brick %s' % node_brick)
        g.log.info('Successfully remounted %s with read-write option',
                   node_brick)

        # start volume with "force" option
        g.log.info('starting volume with "force" option...')
        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertFalse(ret, 'Failed to start volume %s with "force" option'
                         % self.volname)
        g.log.info('Successfully started volume %s with "force" option',
                   self.volname)

        # Validate IO
        g.log.info('Validating IO on all mounts')
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        g.log.info('Successfully Validated IO on all mounts')
        self.io_validation_complete = True
