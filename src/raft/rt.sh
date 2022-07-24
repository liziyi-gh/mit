rm -f *.log

# for i in {1..5}
i=0
while [ -z "${tmp}" ]
do
    ((i=i+1))
    go test -run 2A > tester.${i}.log
    # go test -run TestInitialElection2A > tester.${i}.log
    # go test -run TestReElection2A -race > tester.${i}.log
    # go test -run TestManyElections2A > tester.${i}.log

    tmp=$(grep FAIL tester.${i}.log)
    tmp2=$(grep "DATA RACE" tester.${i}.log)
    if [ ! -z "${tmp2}" ]
    then
        echo "Data race ${i} test"
        cp raft.log raft.${i}.log
    fi

    if [ ! -z "${tmp}" ]
    then
        echo "Failed ${i} test"
        mv raft.log raft.${i}.log
    else
        echo "Pass ${i} test"
        rm -f raft.log
        rm -f tester.${i}.log
    fi
done
