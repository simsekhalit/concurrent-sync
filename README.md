# Concurrent Sync
A concurrent sync tool which works similar to `rsync`. It supports syncing given sources with multiple targets
concurrently.

## Requirements
Python >= 3.8 is required. (CPython and PyPy are both supported)

## Installation
Concurrent Sync can be either installed directly via pip:
```shell
pip install concurrent-sync
```
Or it can be installed from the source:
```shell
git clone https://github.com/simsekhalit/concurrent-sync.git
python3 -m pip install ./concurrent-sync
```

## Manual
```
$ python3 -m csync --help
usage: csync [-h] [--max-memory MAX_MEMORY] [--target TARGET] SOURCE [SOURCE ...] TARGET

A concurrent sync tool which works similar to rsync. It supports syncing given sources with multiple targets concurrently.

positional arguments:
  SOURCE                specify source directories/files
  TARGET                specify target directory

optional arguments:
  -h, --help            show this help message and exit
  --max-memory MAX_MEMORY
                        specify allowed max memory usage as percent
  --target TARGET       specify additional target directories

For more information: https://github.com/simsekhalit/concurrent-sync
```
Concurrent Sync takes one or more source paths and one or more target paths as arguments.
All the given source paths are synced with each given target path concurrently.
* Only the missing or changed files are copied from source to target.
* While checking if a file is changed, its modification time and size are used similar to `rsync`.
* Trailing slash at the end of the source is interpreted in a similar way to `rsync`.

<br>Following **Examples** section clarifies the working mechanics in a more clear way.

## Examples
### 1. One source path and one target path are given

#### Source
```
/mnt/Source
/mnt/Source/File1
/mnt/Source/File2
/mnt/Source/Folder1
/mnt/Source/Folder2
/mnt/Source/Folder2/File3
```

#### Target
```
/mnt/Target
```

Following command is executed:
```shell
python3 -m csync /mnt/Source /mnt/Target
```

After the sync is completed, target becomes:
```
/mnt/Target
/mnt/Target/Source
/mnt/Target/Source/File1
/mnt/Target/Source/File2
/mnt/Target/Source/Folder1
/mnt/Target/Source/Folder2
/mnt/Target/Source/Folder2/File3
```

### 2. Two source paths and one target are given

#### Source 1
```
/mnt/Source1
/mnt/Source1/File1
/mnt/Source1/File2
```

#### Source 2
```
/mnt/Source2
/mnt/Source2/File3
/mnt/Source2/File4
```

#### Target
```
/mnt/Target
```

Following command is executed:
```shell
python3 -m csync /mnt/Source1 /mnt/Source2 /mnt/Target
```

After the sync is completed, target becomes:
```
/mnt/Target
/mnt/Target/Source1
/mnt/Target/Source1/File1
/mnt/Target/Source1/File2
/mnt/Target/Source2
/mnt/Target/Source2/File3
/mnt/Target/Source2/File4
```

### 3. Source with trailing slash and target are given

#### Source
```
/mnt/Source
/mnt/Source/File1
/mnt/Source/File2
```

#### Target
```
/mnt/Target
```

Following command is executed:
```shell
python3 -m csync /mnt/Source/ /mnt/Target
```

After the sync is completed, target becomes:
```
/mnt/Target
/mnt/Target/File1
/mnt/Target/File2
```

### 4. Source and target with common paths
While syncing subdirectories of source paths with target paths, redundant files/folders are removed.

#### Source
```
/mnt/Source
/mnt/Source/Folder
/mnt/Source/Folder/File1
/mnt/Source/Folder/File2
```

#### Target
```
/mnt/Target
/mnt/Target/Source
/mnt/Target/Source/Folder
/mnt/Target/Source/Folder/File3
```

Following command is executed:
```shell
python3 -m csync /mnt/Source /mnt/Target
```

After the sync is completed, target becomes:
```
/mnt/Target
/mnt/Target/Source
/mnt/Target/Source/Folder
/mnt/Target/Source/Folder/File1
/mnt/Target/Source/Folder/File2
```

Since `File3` is no longer in the source path it's deleted from the target as well.

### 5. One source path and two target paths are given

#### Source
```
/mnt/Source
/mnt/Source/File1
/mnt/Source/File2
```

#### Target 1
```
/mnt/Target1
```

#### Target 2
```
/mnt/Target2
```

Following command is executed:
```shell
python3 -m csync /mnt/Source /mnt/Target1 --target /mnt/Target2
```

After the sync is completed, targets become:

#### Target 1
```
/mnt/Target1
/mnt/Target1/Source
/mnt/Target1/Source/File1
/mnt/Target1/Source/File2
```

#### Target 2
```
/mnt/Target2
/mnt/Target2/Source
/mnt/Target2/Source/File1
/mnt/Target2/Source/File2
```
