Build Instructions
==================

Your instructions go here.
- run

 cd path/NetworkingCW-main/src
 javac AzureLabTest.java
 java AzureLabTest

 on the Terminal for the test AzureLabTest.


Working Functionality
=====================

Describe what you think should work.

1. when Querying through the nodes in localTest, all poem verses 0-15 work and pass sucessfully. the read and write are correct at
retiveibing key and stroing key value. writng the key juliet for each verse and read makes all tests work.

2. storing key/value pair address/value and node name when it finds a node it stores it sucessfully in its keyvaluestore

3. It finds the 3 closest nodes, a debug comment is placed that shows you the 3 nodes stored as closest in azurelabtest each time. This also means hashID,
calculate distance works also. Key value data oairs are correclty stored. IF 3 closest nodes work then the nearest request repsonse also words, fidning the existence and the name
also seems to work, the debugging in my node.java class shows the netowrk querying thee nodes.

4. when i asks its 3 closest nodes, relaying is taken place, although not really tested, it should work since the poem shows up. if relaying does not work, brute
force takes place. I know its not really mentioned in the RFC, but it does well handling the UDP limitations or packets losses.
If relay works and sometimes doesnt, its more of a netowrk issue, loss of a node. but brute force is used as the last resprt after directread and relay.

2. AzureLabTest implementaiton is also correct, the whole poem jabberwoky poem shows up. and a message "it works" comes up. The prorgams
sucessfully wrote node address to network and read back the node.This means that Read Write also functions correctly.

3. anoother functionality called tryBruteForceRead is a brute force method to retirve nodes, when read requests or relay does
not work. Since UDP is unprecsnet, there have been many other methods to deal with contacting all nodes in the network to find the key.
Although its exhautive, it helps idenify a potential closest node that was in fact not reonisged by closestaddresses.
in a nutshell, If a also message doesn’t arrive (UDP can lose packets), it tries again up to 3 times with a 5-second wait each time,for more handling.

4. other test CAS and realyTests are being tested. for relaytests