FROM alpine:3.8

# update the existing packages
RUN apk update \
  && apk upgrade \
  && apk add bash \
  && apk add --update wget

ENV HADOOP_VERSION 3.1.1
ENV HADOOP_HOME /usr/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin

RUN wget \
 "http://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" \
 && tar -C /usr/ -xzvf "hadoop-$HADOOP_VERSION.tar.gz" \
 && rm -rf $HADOOP_HOME/share/doc \
 && chown -R root:root $HADOOP_HOME