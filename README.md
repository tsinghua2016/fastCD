Identification of different communities in large
weighted networks is of crucial importance since it helps to
uncover priori unknown functional modules such as topics in
information networks or cyber-communities in social networks.
However, the typical size of networks, such as social network
services or World Wide Web, now counts in millions of nodes
and is computationally complex. This urgently demands feasible
methods and available computing platforms to retrieve their
structure efficiently. To address this problem, we propose an
algorithm Fast Community Detection (FastCD) based on modu-
larity optimization. Furthermore, FastCD easily supports parallel
computation. We implement FastCD with GraphX, which is an
embedded graph processing framework built on top of Apache
Spark. After carrying out comprehensive experiments in a 16-
nodes cluster (32 vCPU) on Amazon EC2, the results indicate
that FastCD not only outperforms the state-of-the-art algorithms
in terms of computation time, but also guarantee the accuracy
of the solutions under different real-world networks commonly
used for efficiency comparison.
