module github.com/msg555/casfs

go 1.16

require (
	bazil.org/fuse v0.0.0-20200524192727-fb710f7dfd05
	github.com/boltdb/bolt v1.3.1
	github.com/spf13/pflag v1.0.5
	golang.org/x/sys v0.0.0-20191210023423-ac6580df4449
)

replace bazil.org/fuse => /home/msg/go/src/github.com/msg555/fuse
