# Goofs
## A distributed file store, in the spirit of Google's distributed file sytem

Goofs is a distributed file system that I developed as part of a course on distributed computing systems.  Goofs is partly inspired by whitepapers published by Google on their highly scalable distributed file system.  Goofs is written in Java and should be easily compiled using the included ant buildscript.  [JavaDoc can also be built using ant and can be found here.](http://www.elliottforney.com/javadoc/goofs/)

Goofs utilizes distributed file storage nodes along with a centralized controller node to manage system resources and a client application to submit, retrieve and modify data in the store.  Some of the features included in goofs include:

* Passive replication
* Fault tolerance and recovery (including the controller node)
* High-performance reads and writes

Please note that goofs was written for graduate coursework and research on distributed systems and is not intended to be used in production environments.

Copyright 2011 Elliott M. Forney
All rights reserved.
