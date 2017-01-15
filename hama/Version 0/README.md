# GoFFish V3
### TODO
1. ~~Make it work for VertexCount example.~~
2. Use generics and refactor code.
3. Separate reader logic from GraphJobRunner. Support three types of InputReader:
    1. Edge List (SNAP format).
    2. ~~VID, PartitionID (METIS O/P) followed by edge list: VID1, VID2~~
    3. JSON Reader: With vertex and edge properties.
    4. Two input files: VID, PartitionID and VID, VProperty followed by VID1, VID2, EProperties
4. ~~Create GraphJobMessage, encapsulation over peer messages. It should support subgraphs to send messages directly to each other.~~
5. ~~(optional) Find if there's another way to transfer vertices while forming subgraphs. Currently it sends vertex ID along with it's adj list in form of a string.~~
6. ~~Multithreaded: Make compute function of subgraphs of a partition run in parallel. Currently it executes it sequentially.~~ Might not be useful.
7. Read graph job class from configuration and run accordingly. Currently the job is hard-coded in GraphJobRunner.java.
