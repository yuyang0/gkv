#!/bin/bash
#==========================================================================
#      Filename:  mkslave.sh
#       Created:  2015-12-10 Thu 18:19
#
#   DESCRIPTION:  
#
#        Author:  Yu Yang
#         Email:  yy2012cn@NOSPAM.gmail.com
#==========================================================================
cd ../extern || exit $?

docker rmi gkv/slave

cat > Dockerfile <<EOF
FROM golang:1.5.2
# upgrade & install required packages
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y \\
    openssh-server bash vim git-core gcc g++ make bzip2 curl wget libsnappy-dev
RUN echo 'root:root' | chpasswd
RUN mkdir -p /var/run/sshd
ENTRYPOINT ["/usr/sbin/sshd", "-D"]
EXPOSE 22
ENV HOMEDIR /gkv
RUN mkdir -p \${HOMEDIR}
RUN groupadd -r gkv && useradd -r -g gkv gkv -s /bin/bash -d \${HOMEDIR}
RUN echo 'gkv:gkv' | chpasswd
ENV BUILDDIR /tmp/gkv
RUN mkdir -p \${BUILDDIR}

# copy & build leveldb source code
ADD 3rd \${BUILDDIR}
WORKDIR \${BUILDDIR}/leveldb-1.18
RUN make clean
RUN make -j
RUN cp --preserve=links libleveldb.* /usr/local/lib
RUN cp -r include/leveldb /usr/local/include/
RUN ldconfig

# build gkv source code

RUN rm -rf \${BUILDDIR}
EXPOSE 6379
RUN chown -R gkv:gkv \${HOMEDIR}
EOF

docker build --force-rm -t gkv/slave . && rm -f Dockerfile

# docker run --name "gkv-slave" -h "gkv-slave" -d -p 6022:22 -p 6079:6379 gkv/slave
