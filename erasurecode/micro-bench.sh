cmd=bin/hadoop org.apache.hadoop.io.erasurecode.BenchmarkTool
testDir=/home/drankye/run

echo "Testing with 1G data, chunk size 1MB"
echo $cmd $testDir 0 1024 1024
$cmd $testDir 0 1024 1024
echo $cmd $testDir 1 1024 1024
$cmd $testDir 1 1024 1024
echo $cmd $testDir 2 1024 1024
$cmd $testDir 2 1024 1024

rm -rf $testDir/*coded*coder*.dat

echo "Testing with 5G data, chunk size 8MB"
echo $cmd $testDir 0 5120 8192
$cmd $testDir 0 5120 8192
echo $cmd $testDir 1 5120 8192
$cmd $testDir 1 5120 8192
echo $cmd $testDir 2 5120 8192
$cmd $testDir 2 5120 8192

rm -rf $testDir/*coded*coder*.dat