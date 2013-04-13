Limitations
=
Aurinko2 uses memory mapped files on all index and document data files, thus the general platform limitation of memory mapped files all apply.

All pointers in data files are singed 32-bit integers, thus the size limit of any index/document data file is 2GB.

Individual document may not be larger than 16MB when first inserted, and may not grow larger than 32MB, which means the absolute maximum size of a single document is 48MB.