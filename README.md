**Work in progress, simply using github to better track progress at the moment.**

### Goals

This project will aim to implement a file-level content-addressed backing store
that can be mounted, imported, exported, shared, and used as an additional
storage engine for container systems.

This will aim to address the below
shortcomings of current union based overlay storage drives:

* Better sharing accross different layers (and within layers), same data = same storage
* Separate metadata from data so metadata updates (e.g. ctime change) no longer breaks data caches
* A single file update doesn't trigger a whole layer's cache to be invalidated
* Similarly pushing and pulling from a remote only requires sending/receiving new files and metadata
* Tree data will be content addressed
* Any tree can be mounted (e.g. subdirectories of other trees)
* No longer have to worry about number
