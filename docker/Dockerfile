FROM loadtheaccumulator/glusto
MAINTAINER loadtheaccumulator@gmail.com
# install glusto-tests libraries
RUN cd /; git clone http://github.com/gluster/glusto-tests
RUN cd /glusto-tests/glustolibs-gluster; python setup.py install
RUN cd /glusto-tests/glustolibs-io; python setup.py install
RUN cd /glusto-tests/glustolibs-misc; python setup.py install
# install gdeploy
#RUN pip install --upgrade pip
#RUN dnf install -y gcc
#RUN pip install --upgrade git+git://github.com/gluster/gdeploy.git
RUN dnf install -y wget
RUN wget -q https://copr-be.cloud.fedoraproject.org/results/sac/gdeploy/fedora-25-x86_64/00549451-gdeploy/gdeploy-2.0.2-7.noarch.rpm
RUN dnf install -y gdeploy-*
