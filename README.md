# CS 425 MP3 - Simple Distributed File System

This project builds build a Simple Distributed File System (SDFS), which is a simplified version of HDFS.

## Usage
(This is a course project, so there are parameters to change if deploved in different cluster:
client.py:change the hostname at line10 to the machine where master.py runs
SDFS_Node.py:change the hostname at line36 to the hostname of the  machine where master.py will run
peer.py: change the line at line 18 to the hostname of the  machine where introducer.py will run
)
### Start Introducer
First, start the introducer to enable machines to join the group. Run the file [*introducer.py*](introducer.py)
```
python introducer.py
```

### Join A Machine or Start the Master
Log onto the machine needed to join, and run the file [*SDFS_Node.py*](SDFS_Node.py)
```
python SDFS_Node.py
```
We appoint the machine with the least ID in membership list to be the master, and the machine satisfying the requirement will start the master automatically.

For any machine, the files in SDFS locate at [*sdfs*](sdfs) and files for local locate at [*local*](local).

### Put File in to SDFS
Log onto any machine which will be acted as client, and run the file [*client.py*](client.py) instruction *'put local_file sdfs_filename'*, for example:
```
python client.py put local_file.txt sdfs_file.txt
```

### Get Files from SDFS
Log onto a machine acted as client. Run the file  [*client.py*](client.py) with instruction *"get sdfs_filename local_filename"*:
```
python client.py get sdfs_file.txt local_file.txt
```

### Delete Files from SDFS
Log onto that machine acted as client. Run the file  [*client.py*](client.py) with instruction *"delete sdfs_filename"*:
```
python client.py delete sdfs_file.txt
```

### List All Machines Where a File Locate
Log onto that machine acted as client. Run the file  [*client.py*](client.py) with instruction *"ls sdfs_filename"*:
```
python client.py ls sdfs_file.txt
```

### List All Files Currently Being sotred at This Machine:
Log onto that machine acted as client. Run the file  [*client.py*](client.py) with instruction *"store"*:
```
python client.py store
```

### Get Latest Several Versions of A File:
Log onto that machine acted as client. Run the file  [*client.py*](client.py) with instruction *"get-versions sdfs_filename num_versions local_filename"*:
```
python client.py get-versions sdfs_file.txt 3 local_file.txt
```
This will get latest 3 versions.


