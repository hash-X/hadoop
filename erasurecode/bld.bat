del sample
del *.obj
cl -Iinclude /DWINDOWS /DWIN32 /D HADOOP_ISAL_LIBRARY=\"isa-l.dll\" erasure_code.c erasure_coder.c erasure_coder_sample.c -o sample
sample 6 3
