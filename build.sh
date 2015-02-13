#!/usr/bin/env sh
find  . -name '*.java' -print0 | \
xargs -0 javac -cp ${HADOOP_HOME}/hadoop-core-1.2.1.jar -d class_dir
jar -cvf ngram.jar -C class_dir/ .
