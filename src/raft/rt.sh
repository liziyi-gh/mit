rm ./*.log

echo "start test at "
date

i=0
while [ -z "${tmp}" ]
do
    ((i=i+1))
    TESTER_LOG_FILE=/tmp/tmp-fs/tester.${i}.log
    RAFT_LOG_FILE=/tmp/tmp-fs/raft.log
    # go test -run 2A > ${TESTER_LOG_FILE}
    # go test -run 2B > ${TESTER_LOG_FILE}
    # go test -run 2C > ${TESTER_LOG_FILE}
    # go test -run TestOnlyOneElectionLZY -race > ${TESTER_LOG_FILE}
    # go test -run TestBasicAgree2B -race > ${TESTER_LOG_FILE}
    # go test -run TestFailAgree2B -race > ${TESTER_LOG_FILE}
    # go test -run TestRPCBytes2B -race > ${TESTER_LOG_FILE}
    # go test -run TestFailNoAgree2B -race > ${TESTER_LOG_FILE}
    # go test -run TestRejoin2B -race > ${TESTER_LOG_FILE}
    # go test -run TestConcurrentStarts2B -race > ${TESTER_LOG_FILE}
    # go test -run TestCount2B -race > ${TESTER_LOG_FILE}
    # go test -run TestBackup2B -race > ${TESTER_LOG_FILE}
    # go test -run TestFigure82C -race > ${TESTER_LOG_FILE}
    # go test -run TestFigure8Unreliable2C -race > ${TESTER_LOG_FILE}
    # go test -run TestReliableChurn2C -race > ${TESTER_LOG_FILE}
    # go test -run TestSnapshotBasic2D -race > ${TESTER_LOG_FILE}
    # go test -run TestSnapshotInstall2D -race > ${TESTER_LOG_FILE}
    # go test -run TestSnapshotInstallUnreliable2D -race > ${TESTER_LOG_FILE}
    # go test -run TestSnapshotInstallCrash2D -race > ${TESTER_LOG_FILE}
    # go test -run TestSnapshotInstallUnCrash2D -race > ${TESTER_LOG_FILE}
    go test -run TestSnapshotAllCrash2D -race > ${TESTER_LOG_FILE}

    tmp=$(rg FAIL ${TESTER_LOG_FILE})
    tmp2=$(rg "DATA RACE" ${TESTER_LOG_FILE})
    if [ ! -z "${tmp2}" ]
    then
        echo "Data race ${i} test"
        mv ${TESTER_LOG_FILE} ./tester.${i}.log
        mv ${RAFT_LOG_FILE} raft.${i}.log
    fi

    if [ ! -z "${tmp}" ]
    then
        echo -e "\033[31mFailed \033[0m${i} test at"
        date
        mv ${TESTER_LOG_FILE} ./tester.${i}.log
        mv ${RAFT_LOG_FILE} raft.${i}.log
    else
        echo -e "\033[32mPass \033[0m${i} test at"
        date
        rm -f ${RAFT_LOG_FILE}
        rm -f ${TESTER_LOG_FILE}
    fi
done
