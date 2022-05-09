i=1
# The exit code of "go test -run $1 | tee log.txt" is incorrect!
if [ $1 ]; then
	while go test -run $1 > log.txt; do
		echo $i
		i=$((i+1))
	done
else
	while go test > log.txt; do
		echo $i
		i=$((i+1))
	done
fi
echo $i failed
