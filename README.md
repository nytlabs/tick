tick
====

"The Tick seems to have no memory of its existence before being The Tick, and indeed not much memory of anything."

Tick is a simple service that exposes a characteristics of a real-time JSON stream. It captures the rate of the occurrence of all the attributes in a stream, tries to infer their type(s) and and builds a time series of each of the attributes and their values.

Consumers of the stream can get an rough idea of what to expect in terms of how many events/minute to expect. What attributes and types are they dealing with and how the attribute values change over time.

Tick is a simple go app that listens to a stream from an NSQ topic and stores all the characteristics information in Cassandra. It also has a helper API to expose the characteristics.

Tick was primariy built for experimental purposes. 
