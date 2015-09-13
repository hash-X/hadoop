rm -rf sample
gcc -Iinclude -D UNIX -D_GNU_SOURCE -D HADOOP_ISAL_LIBRARY=\"libisal.so\" -g -ldl erasure_code.c erasure_coder.c erasure_coder_sample.c -o sample
if [ $? = 0 ]; then
  echo ./sample 6 3
fi
