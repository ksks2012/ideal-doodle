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

# Plan

- [ ]  Log method
- [ ]  Config file
- [ ]  Test

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

### Flow

## Worker

- Filter [A-Z, a-z]
- provide Intermediate files

### Flow

### method

- execute user define Map task
- passed back location of Intermediate file in disk to master

### Struct

- status:
    - idle
    - running
    - fail

# Future

- communicate by grpc
    
    ⇒ different between rpc and grpc
    
- Docker
- File System