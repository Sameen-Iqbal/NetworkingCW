Build Instructions
==================

Your instructions go here.


- run Local

 cd path/NetworkingCW-main/src
 javac LocalTest.java
 java AzureLabTest

 on the Terminal for the test AzureLabTest.


- run AzureLab

 cd path/NetworkingCW-main/src
 javac AzureLabTest.java
 java AzureLabTest

 on the Terminal for the test AzureLabTest.


Working Functionality
=====================

Describe what you think should work.

1.When Querying through the nodes in localTest, all poem verses 0-15 work and pass successfully. The read and write functions correctly retrieve and store key-value pairs.
Writing the key "Juliet" for each verse and reading it back makes all tests pass.

2. Storing key/value pairs When the node finds another node, it successfully stores the node’s name and address (e.g., "N:node1" → "10.216.34.173:20111") in its keyValueStore.
 It also stores data key/value pairs correctly.

3. It finds the 3 closest nodes, a debug comment is placed that shows you the 3 nodes stored as closest in Node.java when run AzureLabTest each time. This also means hashID,
calculate distance works also. Key value data pairs are correctly stored. If 3 closest nodes work then the nearest request response also words, finding the existence and the name
also seems to work.

4. When I ask for its 3 closest nodes, relaying is taken place, although not really tested, it should work since the poem shows up. if relaying hypothetically does not work, brute
force takes place. I know it's not really mentioned in the RFC, but it does well handling the UDP limitations or packets losses.
If relay works and sometimes doesn't, its more of a network issue, like loss of a node in transmission. but brute force is used as the last resort after directRead and relay.

2. AzureLabTest implementation is also correct, the whole poem jabberwocky poem shows up and the message "it works". The program
continues successfully for wrote node address to network and reading back the node.This means that Read Write also functions correctly.

3. another functionality called tryBruteForceRead is a brute force method to retrieve nodes, when read requests or relay does
not work. Since UDP is unprecedented, there have been many other methods to deal with contacting all nodes in the network to find the key.
Although its exhaustive, it helps identify a potential closest node that was in fact not recongnised by closestaddresses.

4. In a nutshell, If also a message does arrive (UDP can lose packets), it tries again up to 3 times with a 5-second wait each time,for more handling.

4. Other test CAS and relayTests are being tested. For relaytests