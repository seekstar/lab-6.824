i=1
# The exit code of "go test -run $1 | tee log.txt" is incorrect!
while go test -run $1 > log.txt; do
    echo $i
    i=$((i+1))
done
echo $i failed
