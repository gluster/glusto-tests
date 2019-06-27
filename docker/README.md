# Glusto in docker
This Dockerfile adds Glusto and the Gluster glustolibs libraries on top of the
Fedora container to provide the complete environment required to run Gluster
QE tests under Glusto. This container is currently based on Fedora and also
installs gdeploy as required by NFS Ganesha tests.

**This is currently a minimal implementation. More to come.**

To use the glusto-tests container, you can run tests using the pre-gen'd
container or create a Dockerfile and customize to fit your own needs.
To use the pre-gen'd image...

- Pull the docker image down to your local system.

```
$ docker pull gluster/glusto-tests
```

- Run the image with docker.

```
$ docker run -it --rm --privileged=true -v <local_dir_path>:<container_dir_path> docker.io/gluster/glusto-tests /bin/bash
```
or for example to run a test directly.

```
$ docker run -it --rm --privileged=true \
  -v $WORKSPACE/:/workspace \
   docker.io/gluster/glusto-tests \
   /usr/bin/glusto -c /workspace/myservers.yml \
  --pytest="-vv /workspace/testdir/test_your_test_file.py"
```

More robust docs and examples coming soon.