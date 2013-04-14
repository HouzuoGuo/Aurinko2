Concurrency and networking
=

# Networking
An Aurinko2 server serves one database instance. Data exchange between client and server is by transmitting XML documents.

Aurinko2 server creates a new worker thread for each incoming connection, worker thread is destroyed only when client connection is closed.

Aurinko2 communication protocol is straight-forward:

- Client request ends with `<go/>` on its own line
- Server response ends with `<ok/>` on its own line

# Concurrency
Aurinko2 is optimised to be scalable on symmetric-multiprocessing architectures.

The only synchronised operations in Aurinko2 are file IOs; each file has a fair IO queue (`LinkedBlockingQueue`) and a worker thread.

No other explicit synchronisation/locks exist. Aurinko2 implementation is mainly lock-free.

# Usage as an embedded database engine
You may get even better performance (sometimes 2x or 3x) with Aurinko2 used as an embedded database engine.

I apologise that there is no detailed documentation on this usage scenario, however some examples can be found at [here][].

[here]: https://github.com/HouzuoGuo/Aurinko2/blob/master/src/test/scala/net/houzuo/aurinko2/test/logic/Benchmark.scala