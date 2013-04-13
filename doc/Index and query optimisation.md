Index and query optimisation
=
This page shows you some performance characteristics of Aurinko2 data storage and query processor.

# Query processing work-flow
Query is an XML element, each child element denotes a query condition. Query conditions are processed recursively, where condition result is a Set of integer.

To process query `Q`, the query processor does:

<pre>For each child element of `Q`:
    If child = `eq` condition:
        If path is indexed:
            Index look-up
        Otherwise:
            Collection scan
    Else if child = `has` condition:
        If path is indexed:
            Index scan all
        Otherwise:
            Collection scan
    Else if child = `all`:
        (ID index) index scan all
    Else if child = `diff`:
        Process child conditions, then reduce to left.
    Else if child = `intersect`:
        Process child conditions, then reduce to left.
</pre>

# Query performance characteristics
## Index
- `Index look-up` is very efficient (30k+ per second)
- `Index scan all` is less efficient (50+ per second, largely depends on hash table size)
- Condition parameter `limit` may help improving performance in index look-ups (`eq` conditions)
- Condition parameter `skip` does not have impact on performance

## Collection
- Collection scan is the slowest, avoid such query conditions if possible.

# Hash index parameters
When creating a hash index, you may explicitly define its bucket size and number of key bits; by default, each bucket holds 100 entries, and 12 right-most bits of value are the hash key. The default settings are optimal for most use cases.

By increasing the number of key bits, `index look-up` may show improved performance, however `index scan all` will likely be slower. To avoid creating a hash table which is mostly empty, I recommend lowering bucket size when raising number of key bits.

Avoid using more than 15 key bits (your hash table will take too much disk space in the case)!

If hash key collision happens very often, minor performance improvement may be gained by raising bucket size. Raising bucket size does not impact `index scan all` performance.