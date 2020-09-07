# NFS Ganesha Tests

Scope of Testing:
Nfs Ganesha functional tests includes test scripts specific to nfs ganesha
component such as high availability, nfsv4 acls, root squash, locks,
volume exports, subdirectory exports from client and server side, dynamic
refresh config.

Configs to change in glusto_tests_config.yml file for running the tests:

In cluster_config -> nfs_ganesha section,
- Set enable: True
- Give the number of nodes to participate in nfs ganesha cluster in
  integer format.
- Virtual IPs for each nodes which will be part of nfs ganesha cluster
  in list format.

In mounts section, for each mount
- Set protocol to 'nfs'.
- For v3 mount, set options: 'vers=3'
- For v4 mount, set options: 'vers=4.0'
- If 'options' is set to empty string, it takes v3 mount by default.
