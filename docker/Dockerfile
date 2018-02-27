FROM fedora:26
MAINTAINER loadtheaccumulator@gmail.com

# install gdeploy first due to pip/rpm PyYAML conflict
RUN dnf install -y wget
RUN wget -q https://copr.fedorainfracloud.org/coprs/sac/gdeploy/repo/fedora-26/ -O /etc/yum.repos.d/gdeploy.repo
RUN dnf install -y gdeploy*

# install glusto
RUN dnf install -y git
RUN dnf install -y python-pip
RUN pip install --upgrade pip
RUN pip install --upgrade git+git://github.com/loadtheaccumulator/glusto.git
RUN mkdir /etc/glusto/
COPY defaults/defaults.yml /etc/glusto/

# install glusto-tests libraries
RUN cd /; git clone http://github.com/gluster/glusto-tests
RUN cd /glusto-tests/glustolibs-gluster; python setup.py install
RUN cd /glusto-tests/glustolibs-io; python setup.py install
RUN cd /glusto-tests/glustolibs-misc; python setup.py install

# install dev extras
RUN dnf install -y pylint
RUN dnf install -y python-pep8

# install code coverage extras
RUN dnf install -y lcov
RUN pip install gcovr
