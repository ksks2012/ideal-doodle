# Lab 1 MapReduce

# **Getting started**

- Supplied code
    
    ```rust
    src/main/mrsequential.go
    ```
    
    - word-count
        
        ```rust
        mrapps/wc.go
        ```
        
    - text indexer
        
        ```rust
        mrapps/indexer.go
        ```
        
- Output files
    
    ```rust
    mr-out-{count}
    ```
    
- Input files
    
    ```rust
    pg-{name}.txt
    ```
    
- Suppose we have a shared file system

# Job

- implement a distributed MapReduce
    - master
    - worker
- Action
    - map
    - reduce
        - job id is bucket number
    - exit
        - no job in map and reduce queue

# Plan

- [ ]  Log method
- [ ]  Config file
- [ ]  Test
- [ ]  Time spend
- [ ]  Timeout
- [ ]  Lock

# Thinking

- map[worker, block] = data
- file size → split to block
    - line
    - keys (file count, block count)
- **Shuffle ?**

## Master

- only one →controller

### method

- pick idle worker → regist
- restore worker list
- queue data states map reduce
- parse key-value of split data to worker
- restore location of Intermediate file

### Method implement

- Regist
    - args
        - worker id
    - reply
        - sucess or not
    - sate
        - worker: init
- GetJob
    - args
        - worker id
    - reply
        - Job
    - sate
        - worker: not init → exit
    - flow
        1. Check **Worker** is Regist
        2. Check **Worker** init and **Job** exist
        3. Assignment Job to Worker
    - hint
        - If no 'map job', return 'reduce job'
        - Number of reduce Job is base on keys
- Report
    - send work result to master
    - Return job success
        - adadd done job to done queue

### Flow

## Worker

- Filter [A-Z, a-z]
- provide Intermediate files

### Flow

- Get job
- Read file
- Save work result
- Apply the work
- exit signal from master

### method

- execute user define Map task
- passed back location of Intermediate file in disk to master

### Struct

- status:
    - idle
    - running
    - fail

# Build

## Build wc.go

```go
// $>main
go build -buildmode=plugin ./wc.go
```

# Run

## master

```go
// go run mrmaster.go ${regex of input file}
go run mrmaster.go pg-*.txt
```

## worker

```go
// go run ./main/mrworker.go {plugin *.so}
go run ./main/mrworker.go wc.so
```

# Test

- Basic test, a mater with three workers
    
    ```go
    ./main/test-mr.sh basic
    ```
    

# Map

# Reduce

- read bucket files
- collect same key in bucket files
- read file by bucket

# Future

- communicate by grpc
    
    ⇒ different between rpc and grpc
    
- Docker
- File System
- message queue

# Work Record

- 5/7: try to pass test script
    - worker will end on master end
    - flow to done
    - lock for other test