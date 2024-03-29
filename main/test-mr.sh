#!/bin/bash

#
# basic map-reduce test
#

RACE=

# uncomment this to run the tests with the Go race detector.
#RACE=-race

function clear_output() {
    # run the test in a fresh sub-directory.
    echo "*** clearing output"
    rm -rf mr-tmp
    mkdir mr-tmp || exit 1
    cd mr-tmp || exit 1
    rm -f mr-*
    echo "*** cleared output"
}

function build() {
    # make sure software is freshly built.
    echo "*** building"
    (cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
    (cd ../../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
    (cd ../../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
    (cd ../../mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
    (cd ../../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
    (cd ../../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
    (cd .. && go build $RACE mrmaster.go) || exit 1
    (cd .. && go build $RACE mrworker.go) || exit 1
    (cd .. && go build $RACE mrsequential.go) || exit 1
    echo "*** builded"
    failed_any=0
}

# first word-count

function generate_correct() {
    # generate the correct output
    echo "*** Generating correct output"
    ../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
    sort mr-out-0 > mr-correct-wc.txt
    rm -f mr-out*
    echo "*** Generated correct output"
}

function basic_test() {
    echo '***' Starting basic test.

    timeout -k 2s 180s ../mrmaster ../pg*txt &

    # give the master time to create the sockets.
    sleep 1

    # start multiple workers.
    timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
    timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
    timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &

    # wait for one of the processes to exit.
    # under bash, this waits for all processes,
    # including the master.
    wait

    # the master or a worker has exited. since workers are required
    # to exit when a job is completely finished, and not before,
    # that means the job has finished.

    sort mr-out* | grep . > mr-wc-all
    if cmp mr-wc-all mr-correct-wc.txt
    then
        echo '---' wc test: PASS
    else
        echo '---' wc output is not the same as mr-correct-wc.txt
        echo '---' wc test: FAIL
        failed_any=1
    fi

    # wait for remaining workers and master to exit.
    wait ; wait ; wait
}

function multiple() {
    # now indexer
    rm -f mr-*

    # generate the correct output
    ../mrsequential ../../mrapps/indexer.so ../pg*txt || exit 1
    sort mr-out-0 > mr-correct-indexer.txt
    rm -f mr-out*

    echo '***' Starting indexer test.

    timeout -k 2s 180s ../mrmaster ../pg*txt &
    sleep 1

    # start multiple workers
    timeout -k 2s 180s ../mrworker ../../mrapps/indexer.so &
    timeout -k 2s 180s ../mrworker ../../mrapps/indexer.so

    sort mr-out* | grep . > mr-indexer-all
    if cmp mr-indexer-all mr-correct-indexer.txt
    then
        echo '---' indexer test: PASS
    else
        echo '---' indexer output is not the same as mr-correct-indexer.txt
        echo '---' indexer test: FAIL
        failed_any=1
    fi

    wait ; wait
}

function parallmap() {
    echo '***' Starting map parallelism test.

    rm -f mr-out* mr-worker*

    timeout -k 2s 180s ../mrmaster ../pg*txt &
    sleep 1

    timeout -k 2s 180s ../mrworker ../../mrapps/mtiming.so &
    timeout -k 2s 180s ../mrworker ../../mrapps/mtiming.so

    NT=`cat mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
    if [ "$NT" != "2" ]
    then
        echo '---' saw "$NT" workers rather than 2
        echo '---' map parallelism test: FAIL
        failed_any=1
    fi

    if cat mr-out* | grep '^parallel.* 2' > /dev/null
    then
        echo '---' map parallelism test: PASS
    else
        echo '---' map workers did not run in parallel
        echo '---' map parallelism test: FAIL
        failed_any=1
    fi

    wait ; wait
}

function parallreduce() {
    echo '***' Starting reduce parallelism test.

    rm -f mr-out* mr-worker*

    timeout -k 2s 180s ../mrmaster ../pg*txt &
    sleep 1

    timeout -k 2s 180s ../mrworker ../../mrapps/rtiming.so &
    timeout -k 2s 180s ../mrworker ../../mrapps/rtiming.so

    NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
    if [ "$NT" -lt "2" ]
    then
        echo '---' too few parallel reduces.
        echo '---' reduce parallelism test: FAIL
        failed_any=1
    else
        echo '---' reduce parallelism test: PASS
    fi

    wait ; wait


    # generate the correct output
    ../mrsequential ../../mrapps/nocrash.so ../pg*txt || exit 1
    sort mr-out-0 > mr-correct-crash.txt
    rm -f mr-out*
}

function crash() {
    echo '***' Starting crash test.

    rm -f mr-done
    (timeout -k 2s 180s ../mrmaster ../pg*txt ; touch mr-done ) &
    sleep 1

    # start multiple workers
    timeout -k 2s 180s ../mrworker ../../mrapps/crash.so &

    # mimic rpc.go's masterSock()
    SOCKNAME=/var/tmp/824-mr-`id -u`

    ( while [ -e $SOCKNAME -a ! -f mr-done ]
        do
        timeout -k 2s 180s ../mrworker ../../mrapps/crash.so
        sleep 1
        done ) &

    ( while [ -e $SOCKNAME -a ! -f mr-done ]
        do
        timeout -k 2s 180s ../mrworker ../../mrapps/crash.so
        sleep 1
        done ) &

    while [ -e $SOCKNAME -a ! -f mr-done ]
    do
        timeout -k 2s 180s ../mrworker ../../mrapps/crash.so
        sleep 1
    done

    wait
    wait
    wait

    rm $SOCKNAME
    sort mr-out* | grep . > mr-crash-all
    if cmp mr-crash-all mr-correct-crash.txt
    then
        echo '---' crash test: PASS
    else
        echo '---' crash output is not the same as mr-correct-crash.txt
        echo '---' crash test: FAIL
        failed_any=1
    fi

    if [ $failed_any -eq 0 ]; then
        echo '***' PASSED ALL TESTS
    else
        echo '***' FAILED SOME TESTS
        exit 1
    fi
}

function all() {
    echo '***' Starting all of test.
    basic_test
    multiple
    parallmap
    parallreduce
    crash
}

function help() {
    echo "Enable flags:"
    echo "- all: run all of the tests"
    echo "- basic: basic test"
    echo "- multiple: test with indexer.so"
    echo "- parallmap: run with map parallelism test"
    echo "- parallreduce: run with reduce parallelism test"
    echo "- crash: run with crash.so"
}


# Prepare Stage for all tests
if [ -z "$1" ]
then
    help
elif [ "$1" == "help" ]
then
    help
else
    clear_output
    build
    generate_correct

    case "$1" in
        all)
        all
        ;;
        basic) 
        basic_test
        ;;
        multiple)
        multiple
        ;;
        parallmap)
        parallmap
        ;;
        parallreduce)
        parallreduce
        ;;
        crash)
        crash
        ;;
        *) echo "*** unknow"
    esac
fi