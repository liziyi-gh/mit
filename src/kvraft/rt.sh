test_path=/tmp/tmp-fs

rm ./*.log
rm -f ${test_path}/*.log

echo "start test at "
date

i=0
while [ -z "${tmp}" ]
do
    ((i=i+1))
    TESTER_LOG_FILE=${test_path}/tester.${i}.log
    RAFT_LOG_FILE=${test_path}/raft.log

    go test -run TestBasic3A -race > ${TESTER_LOG_FILE}

    tmp=$(rg FAIL ${TESTER_LOG_FILE})
    tmp2=$(rg "DATA RACE" ${TESTER_LOG_FILE})
    if [ ! -z "${tmp2}" ]
    then
        echo "Data race ${i} test"
        # mv ${TESTER_LOG_FILE} ./tester.${i}.log
        # mv ${RAFT_LOG_FILE} raft.${i}.log
    fi

    if [ ! -z "${tmp}" ]
    then
        echo -e "\033[31mFailed \033[0m${i} test at"
        date
        # mv ${TESTER_LOG_FILE} ./tester.${i}.log
        # mv ${RAFT_LOG_FILE} raft.${i}.log
    else
        echo -e "\033[32mPass \033[0m${i} test at"
        date
        rm -f ${RAFT_LOG_FILE}
        rm -f ${TESTER_LOG_FILE}
    fi
done
