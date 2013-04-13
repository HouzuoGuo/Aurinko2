Data Structure
=

# General
An Aurinko2 server serves one database. A database has many collections. A collection has a series of documents, a number of indexes, and an ID index.

Aurinko2 data structures are designed to be corruption-resistant; any amount of corrupted data will not degrade availability of healthy data.

# Database
Database is a file system directory.

Aurinko2 require RWX (read/write/execute) permissions on the directory.

# Collection
Collection is a file system directory under Database directory.

Collection directory has the following files:

- `data` - document content
- `id` - document ID index
- User-created indexes

Aurinko2 server require RW permissions on those files.

# Collection data file
Data file contains all document content, new documents are inserted to end-of-data* and are left room for future updates.

Deleted documents are marked as deleted. Executing a server command will recover the lost space.

Updating document overwrites original document in-place; or delete and re-insert the document if there was no place left for growing.

\* not to be confused with end-of-file

Data file is initially 64MB. It grows automatically by 64MB when end-of-data meets end-of-file.

### Document format
There is no padding before or after a document. Each document is stored in the following format:
<table style="width: 100%;">
  <tr>
    <th>Type</th>
    <th>Size (bytes)</th>
    <th>Description</th>
    <th></th>
  </tr>
  <tr>
    <td>Signed Int</td>
    <td>4</td>
    <td>Validity</td>
    <td>0 - deleted, 1 - valid</td>
  </tr>
  <tr>
    <td>Signed Int</td>
    <td>4</td>
    <td>Allocated room</td>
    <td>(Max) Room for the document to grow</td>
  </tr>
  <tr>
    <td>Char Array</td>
    <td>Size of document content</td>
    <td>Document content</td>
    <td>Encoded in UTF-8</td>
  </tr>
  <tr>
    <td>Char Array</td>
    <td>Size of document content * 2</td>
    <td>Padding (UTF-8 spaces)</td>
    <td>Room for future updates, for the document to grow its size</td>
  </tr>
</table>

# Index file
Aurinko2 alhpha release supports hash index for look-up queries.

Index file is a typical static hash table made of entry buckets (all buckets have same number of entries); overflowing entries cause new buckets to be chained.

Index file is initially 64MB. It grows automatically by 64MB when end-of-data meets end-of-file.

### Bucket format
There is no padding before or after a bucket. Each bucket is stored in the following format:
<table style="width: 100%;">
  <tr>
    <th>Type</th>
    <th>Size (bytes)</th>
    <th>Description</th>
    <th></th>
  </tr>
  <tr>
    <td>Signed Int</td>
    <td>4</td>
    <td>Next chained bucket number</td>
    <td>0 indicates there is no chained bucket</td>
  </tr>
  <tr>
    <td>Bucket Entry</td>
    <td>12 * N</td>
    <td>Bucket entries</td>
    <td>See "Bucket entry format"</td>
  </tr>
</table>

### Bucket entry format
There is no padding before or after an entry. Each entry is stored in the following format:
<table style="width: 100%;">
  <tr>
    <th>Type</th>
    <th>Size (bytes)</th>
    <th>Description</th>
    <th></th>
  </tr>
  <tr>
    <td>Signed Int</td>
    <td>4</td>
    <td>Validity</td>
    <td>0 - deleted, 1 - valid</td>
  </tr>
  <tr>
    <td>Signed Int</td>
    <td>4</td>
    <td>Key</td>
    <td>An integer key</td>
  </tr>
  <tr>
    <td>Signed Int</td>
    <td>4</td>
    <td>Value</td>
    <td>An integer value</td>
  </tr>
</table>

# ID index
It is an ordinary hash index file with keys and values set to all document IDs.

Many common queries use ID index, such as:

- Get all document IDs
- Count number of documents in a collection