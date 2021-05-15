**Work in progress, simply using github to better track progress at the moment.**

### Goals

This project will aim to implement a file-level content-addressed backing store
that can be mounted, imported, exported, shared, and used as an additional
storage engine for container systems.

ctrfs is designed to be the engine for read/write container file systems. It is
specialized for containers by observing the following special requirements of
container images.

* There is a high degree of data duplication between layers of different images.
 * Data is deduplicated at the block level across all layers/images
 * Pushing/pulling to registry is done at the file level using content addressing with metadata stored separately
* Overlay copy-up can be slow on file systems that do not support reflinks (check/measure this)
 * Persistent data structures are used and data is "copied-up" a single block at a time
 * Only dirty blocks need to be written back to central storage on commit.
* Image building is the most common reason to write to the container layer.
 * There is no journaling
 * fsync calls are ignored by default
 * All writes are asynchronous and written only to cache sychronously
 * Important persistant data used by a container should be written to a separate volume
* ctrfs also fixes some compatibility issues
 * Hardlinks work as expected after committing

Other notes
* Tree data will be content addressed
* Hardlinks are supported through special inode remapping structures
