# Hashcheck

Hashcheck provides command line tools to compute hash codes of large
number of files and to compare them to another set of files or the
same files later on to make sure that the files have not been
corrupted in the storage medium.

On Linux you can use zfs file system and it's scrubbing
functionality. It efficiently compares data to hashes on the block
level. If you can not use zfs, you must calculate and compare the
hashes by other means. If you have lot's of small files on hard disk
drive, this can easily take days because the files can not be read in
the order they reside the disk and the read head has to constantly
jump around.

[hashdeep|https://github.com/jessek/hashdeep] is another similar
tool. The main difference is that hashcheck can be interrupted and can
resume the hash calculation.

The hashes are identified by the file path relative to the archive
directory root. That way you can compare two archives residing in
different directories or disks.

# Deployment

To build and deploy to `~/bin` run `deploy.sh`.

To build and run without deployment run:

```
lein uberjar
java -jar target/hashcheck.jar
```

# Usage

Run without parameters to get list of the available commands.

The commands are:

```
write-file-hashes: ([archive-directory-path output-file-path] [archive-directory-path source-directory-path output-file-path])

  Write hashes to a file.

  examples:
  hashcheck write-file-hashes target target-hashes.edn

  To write hashes of only some part of the whole archive, add the subdirectory as the second argument:
  hashcheck write-file-hashes target target/classes target-hashes.edn

compare-hash-files: ([hash-file-1-path hash-file-2-path])

  Sum up differences between hash files as numbers.

list-differing-hashes: ([hash-file-1-path hash-file-2-path])

  List files with differing hashes.

print-statistics: ([hash-file-path])

  Print how many files and how many bytes there are by file extension.
```
