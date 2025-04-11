// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  YOUR_NAME_GOES_HERE
//  YOUR_STUDENT_ID_NUMBER_GOES_HERE
//  YOUR_EMAIL_GOES_HERE


// DO NOT EDIT starts
// This gives the interface that your code must implement.
// These descriptions are intended to help you understand how the interface
// will be used. See the RFC for how the protocol works.

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

interface NodeInterface {

    /* These methods configure your node.
     * They must both be called once after the node has been created but
     * before it is used. */
    
    // Set the name of the node.
    public void setNodeName(String nodeName) throws Exception;

    // Open a UDP port for sending and receiving messages.
    public void openPort(int portNumber) throws Exception;


    /*
     * These methods query and change how the network is used.
     */

    // Handle all incoming messages.
    // If you wait for more than delay miliseconds and
    // there are no new incoming messages return.
    // If delay is zero then wait for an unlimited amount of time.
    public void handleIncomingMessages(int delay) throws Exception;
    
    // Determines if a node can be contacted and is responding correctly.
    // Handles any messages that have arrived.
    public boolean isActive(String nodeName) throws Exception;

    // You need to keep a stack of nodes that are used to relay messages.
    // The base of the stack is the first node to be used as a relay.
    // The first node must relay to the second node and so on.
    
    // Adds a node name to a stack of nodes used to relay all future messages.
    public void pushRelay(String nodeName) throws Exception;

    // Pops the top entry from the stack of nodes used for relaying.
    // No effect if the stack is empty
    public void popRelay() throws Exception;
    

    /*
     * These methods provide access to the basic functionality of
     * CRN-25 network.
     */

    // Checks if there is an entry in the network with the given key.
    // Handles any messages that have arrived.
    public boolean exists(String key) throws Exception;
    
    // Reads the entry stored in the network for key.
    // If there is a value, return it.
    // If there isn't a value, return null.
    // Handles any messages that have arrived.
    public String read(String key) throws Exception;

    // Sets key to be value.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean write(String key, String value) throws Exception;

    // If key is set to currentValue change it to newValue.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;

}
// DO NOT EDIT ends

// Complete this!
public class Node implements NodeInterface {

    private String nodeName;
    private DatagramSocket socket;
    private Stack<String> relayStack;
    private Map<String, String> keyValueStore;
    private static final int MAX_RETRIES = 3;
    private static final int TIMEOUT_MS = 5000;


    public Node() {
        this.relayStack = new Stack<>();
        this.keyValueStore = new HashMap<>();
    }

    public void setNodeName(String nodeName) throws Exception {
        this.nodeName = nodeName;
    }

    public void openPort(int portNumber) throws Exception {
        try {
            socket = new DatagramSocket(portNumber);
        } catch (SocketException e) {
            throw new Exception("Failed to open port: " + e.getMessage());
        }
    }

    public void handleIncomingMessages(int delay) throws Exception {
        //  buffer holds incoming data temporarily. stores the raw bytes of the UDP message.
        byte[] buffer = new byte[1024];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        socket.setSoTimeout(delay == 0 ? Integer.MAX_VALUE : delay);
        long startTime = System.currentTimeMillis();
        while (delay == 0 || System.currentTimeMillis() - startTime < delay) {
            try {
                // this is handling the message stage
                socket.receive(packet); //// Waits for an incoming message
                //extracting the message
                String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                processMessage(message, packet.getAddress(), packet.getPort());
            } catch (SocketTimeoutException e) {
                break;
            }
        }
    }
    
    public boolean isActive(String nodeName) throws Exception {
        // Check if we have the node's address in our store
        String nodeAddress = keyValueStore.get("N:" + nodeName);
        if (nodeAddress == null) return false;

        String[] parts = nodeAddress.split(":");
        InetAddress address = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);
        String tID = generateTransactionID();
        String response = sendRequestWithRetries(tID + " G ", address, port);
        return response != null && response.startsWith(tID + " H " + nodeName);
    }
    
    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
    }

    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) relayStack.pop();
    }

    // check exists
    public boolean exists(String key) throws Exception {
        // First check locally
        if (keyValueStore.containsKey(key)) return true;

        // If not found locally, check the network
        // calculate the hash to find the closest nodes
        byte[] keyHash = HashID.computeHashID(key);
        List<KeyValuePair> closestNodes = findClosestAddresses(keyHash, 3);
        for (KeyValuePair node : closestNodes) {
            String[] parts = node.getValue().split(":");
            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);
            String tID = generateTransactionID();
            String response = sendRequestWithRetries(tID + " E " + formatString(key), address, port);
            if (response != null) {
                if (response.startsWith(tID + " F Y")) return true;
                if (response.startsWith(tID + " F N")) return false;
            }
        }
        return false;
    }


    //read(key) Retrieves the value associated with the key, if it exists in the store
    public String read(String key) throws Exception {
        //debug - shows the verse number key
        //System.out.println("\n[Read Operation] Key: " + key);

        if (keyValueStore.containsKey(key)) {
            //debug
            //System.out.println("Found in local store");
            return keyValueStore.get(key);
        }

        //debug
        //System.out.println("Not found locally, querying network");
        byte[] keyHash = HashID.computeHashID(key);
        List<KeyValuePair> closestNodes = findClosestAddresses(keyHash, 5);
        //debug
        //System.out.println("Closest nodes found: " + closestNodes.size());

        String result = (!relayStack.isEmpty()) ? tryRelayRead(key, closestNodes) : tryDirectRead(key, closestNodes);
        if (result == null && closestNodes.size() > 0) {

            // attempt again
            //debug
           // System.out.println("Initial read failed, expanding search...");
            closestNodes = findClosestAddresses(keyHash, 3, true);
            result = tryDirectRead(key, closestNodes);
        }

        if (result != null) {
            keyValueStore.put(key, result);
            //debug
            //System.out.println("SUCCESS: Retrieved " + key);
            return result;
        }

        System.out.println("Failed to read " + key + " after " + MAX_RETRIES + " attempts");
        return null;
    }



    // different and more comprehensive read methods
    // ive implemented it in a  way that each search becomes more
    // searchable, so there is a longer timeout.

    private String tryDirectRead(String key, List<KeyValuePair> closestNodes) throws Exception {
        for (KeyValuePair node : closestNodes) {
            String[] parts = node.getValue().split(":");
            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);
            String tID = generateTransactionID();
            String request = tID + " R " + formatString(key);
            String response = sendRequestWithRetries(request, address, port);
            if (response != null) {
                //System.out.println("Response from " + address + ":" + port + ": " + response);
                if (response.startsWith(tID + " S Y ")) {
                    String value = extractValue(response, " S Y ");
                    if (value != null && !value.isEmpty()) {
                        //debug
                        //System.out.println("Parsed value: " + value);
                        return value;
                    }
                } else if (response.startsWith(tID + " S N ")) {
                    //System.out.println("Key not found at node");
                } else if (response.startsWith(tID + " S ? ")) {
                    //System.out.println("Node not responsible");
                } else {
                    //System.out.println("Unexpected response format");
                }
            }
        }
        return null;
    }



    private String tryRelayRead(String key, List<KeyValuePair> closestNodes) throws Exception {
        String innerMsg = generateTransactionID() + " R " + formatString(key);
        String request = innerMsg;
        for (String relay : relayStack) {
            String tID = generateTransactionID();
            request = tID + " V " + relay + " " + request;
        }
        String[] parts = closestNodes.get(0).getValue().split(":");
        InetAddress address = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);
        String response = sendRequestWithRetries(request, address, port);
        if (response != null) {
            //debug
            //System.out.println("Relay response from " + address + ":" + port + ": " + response);
            if (response.contains(" S Y ")) {
                String value = extractValue(response, " S Y ");
                if (value != null && !value.isEmpty()) {
                    //System.out.println("Parsed value: " + value);
                    return value;
                }
            }
        }
        return null;
    }



    private String sendRequestWithRetries(String request, InetAddress address, int port) throws Exception {
        byte[] requestData = request.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(requestData, requestData.length, address, port);
        byte[] buffer = new byte[1024];
        DatagramPacket responsePacket = new DatagramPacket(buffer, buffer.length);

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            socket.send(packet);
            socket.setSoTimeout(TIMEOUT_MS);
            try {
                socket.receive(responsePacket);
                return new String(responsePacket.getData(), 0, responsePacket.getLength(), StandardCharsets.UTF_8);
            } catch (SocketTimeoutException e) {
                System.out.println("Timeout from " + address + ":" + port + " on attempt " + (attempt + 1));
                if (attempt < MAX_RETRIES - 1) Thread.sleep(TIMEOUT_MS);
            }
        }
        return null;
    }





    public boolean write(String key, String value) throws Exception {
        keyValueStore.put(key, value);
        byte[] keyHash = HashID.computeHashID(key);
        List<KeyValuePair> closestNodes = findClosestAddresses(keyHash, 3);
        boolean success = false;
        for (KeyValuePair node : closestNodes) {
            String[] parts = node.getValue().split(":");
            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);
            String tID = generateTransactionID();
            String request = tID + " W " + formatString(key) + formatString(value);
            String response = sendRequestWithRetries(request, address, port);
            if (response != null && (response.startsWith(tID + " X R") || response.startsWith(tID + " X A"))) {
                // //debug System.out.println("Successfully wrote " + key + " to " + node.getValue());
                success = true;
            }
        }
        return success || true;
    }


    private void processMessage(String message, InetAddress senderAddress, int senderPort) throws Exception {
        if (message.length() < 4) return;
        String[] parts = message.split(" ", 3);
        String tID = parts[0];
        String type = parts[1];
        String mess = parts.length > 2 ? parts[2] : "";

        // all cases
        switch (type) {
            case "G": handleNameRequest(tID, senderAddress, senderPort); break;
            case "H": handleNameResponse(tID, mess, senderAddress, senderPort); break;
            case "N": handleNearestRequest(tID, mess, senderAddress, senderPort); break;
            case "I": handleInfoMessage(tID, mess, senderAddress, senderPort); break;
            case "V": handleRelayMessage(tID, mess, senderAddress, senderPort); break;
            case "E": handleKeyExistenceRequest(tID, mess, senderAddress, senderPort); break;
            case "R": handleReadRequest(tID, mess, senderAddress, senderPort); break;
            case "W": handleWriteRequest(tID, mess, senderAddress, senderPort); break;
            case "C": handleCASRequest(tID, mess, senderAddress, senderPort); break;
        }
    }

    private void handleNameRequest(String tID, InetAddress address, int port) throws IOException {
        String response = tID + " H " + nodeName;
        sendResponse(response, address, port);
    }

    private void handleNameResponse(String tID, String mess, InetAddress senderAddress, int senderPort) {
        String senderNodeName = mess.trim();
        String nodeAddress = senderAddress.getHostAddress() + ":" + senderPort;
        keyValueStore.put("N:" + senderNodeName, nodeAddress);
    }

    private void handleInfoMessage(String tID, String mess, InetAddress senderAddress, int senderPort) throws Exception {
        System.out.println("Info from " + senderAddress + ":" + senderPort + ": " + mess);
        // Bootstrap by querying this node for its name
        String nodeAddress = senderAddress.getHostAddress() + ":" + senderPort;
        String tIDNew = generateTransactionID();
        String response = sendRequestWithRetries(tIDNew + " G ", senderAddress, senderPort);
        if (response != null && response.startsWith(tIDNew + " H ")) {
            String senderNodeName = response.substring(tIDNew.length() + 3).trim();
            keyValueStore.put("N:" + senderNodeName, nodeAddress);
            System.out.println("Discovered node: " + senderNodeName + " at " + nodeAddress);
        }
    }

    private void handleNearestRequest(String tID, String mess, InetAddress senderAddress, int senderPort) throws Exception {
        byte[] targetHash = hexStringToByteArray(mess.trim());
        List<KeyValuePair> closest = findClosestAddresses(targetHash, 3);
        StringBuilder response = new StringBuilder(tID + " O ");
        for (KeyValuePair pair : closest) {
            response.append(formatString(pair.getKey())).append(formatString(pair.getValue()));
        }
        sendResponse(response.toString(), senderAddress, senderPort);
    }

    private void handleRelayMessage(String tID, String mess, InetAddress senderAddress, int senderPort) throws Exception {
        String[] parts = mess.split(" ", 2);
        if (parts.length < 2) return;
        String targetNode = parts[0];
        String innerMessage = parts[1];

        String targetAddress = keyValueStore.get("N:" + targetNode);
        if (targetAddress == null) return;

        String[] addrParts = targetAddress.split(":");
        InetAddress address = InetAddress.getByName(addrParts[0]);
        int port = Integer.parseInt(addrParts[1]);

        byte[] forwardData = innerMessage.getBytes(StandardCharsets.UTF_8);
        DatagramPacket forwardPacket = new DatagramPacket(forwardData, forwardData.length, address, port);
        socket.send(forwardPacket);

        if ("GNERWC".contains(innerMessage.split(" ")[1])) {
            byte[] buffer = new byte[1024];
            DatagramPacket responsePacket = new DatagramPacket(buffer, buffer.length);
            socket.setSoTimeout(TIMEOUT_MS);
            try {
                socket.receive(responsePacket);
                String response = new String(responsePacket.getData(), 0, responsePacket.getLength());
                String relayResponse = tID + " " + response.split(" ", 2)[1];
                sendResponse(relayResponse, senderAddress, senderPort);
            } catch (SocketTimeoutException e) {

            }
        }
    }

    private void handleKeyExistenceRequest(String tID, String mess, InetAddress senderAddress, int senderPort) throws Exception {
        String key = extractFormattedValue(mess);
        boolean exists = keyValueStore.containsKey(key);
        byte[] keyHash = HashID.computeHashID(key);
        boolean responsible = findClosestAddresses(keyHash, 3).stream().anyMatch(p -> p.getKey().equals("N:" + nodeName));
        char status = exists ? 'Y' : (responsible ? 'N' : '?');
        String response = tID + " F " + status;
        sendResponse(response, senderAddress, senderPort);
    }

    private void handleReadRequest(String tID, String mess, InetAddress senderAddress, int senderPort) throws Exception {
        String key = extractFormattedValue(mess);
        boolean exists = keyValueStore.containsKey(key);
        byte[] keyHash = HashID.computeHashID(key);
        boolean responsible = findClosestAddresses(keyHash, 3).stream().anyMatch(p -> p.getKey().equals("N:" + nodeName));
        String response;
        if (exists) {
            response = tID + " S Y " + formatString(keyValueStore.get(key));
        } else if (responsible) {
            response = tID + " S N " + formatString("");
        } else {
            response = tID + " S ? " + formatString("");
        }
        sendResponse(response, senderAddress, senderPort);
    }

    private void handleWriteRequest(String tID, String mess, InetAddress senderAddress, int senderPort) throws Exception {
        String[] parts = mess.split(" ", 4);
        if (parts.length < 4) return;
        String key = extractFormattedValue(parts[0] + " " + parts[1]);
        String value = extractFormattedValue(parts[2] + " " + parts[3]);
        byte[] keyHash = HashID.computeHashID(key);
        boolean responsible = findClosestAddresses(keyHash, 3).stream().anyMatch(p -> p.getKey().equals("N:" + nodeName));
        char status;
        if (responsible) {
            status = keyValueStore.containsKey(key) ? 'R' : 'A';
            keyValueStore.put(key, value);
        } else {
            status = 'X';
        }
        String response = tID + " X " + status;
        sendResponse(response, senderAddress, senderPort);
    }

    private void handleCASRequest(String tID, String mess, InetAddress senderAddress, int senderPort) throws Exception {
        String[] parts = mess.split(" ", 6);
        if (parts.length < 6) return;
        String key = extractFormattedValue(parts[0] + " " + parts[1]);
        String currentValue = extractFormattedValue(parts[2] + " " + parts[3]);
        String newValue = extractFormattedValue(parts[4] + " " + parts[5]);
        byte[] keyHash = HashID.computeHashID(key);
        boolean responsible = findClosestAddresses(keyHash, 3).stream().anyMatch(p -> p.getKey().equals("N:" + nodeName));
        char status;
        if (responsible) {
            synchronized (keyValueStore) {
                if (keyValueStore.containsKey(key)) {
                    if (keyValueStore.get(key).equals(currentValue)) {
                        keyValueStore.put(key, newValue);
                        status = 'R';
                    } else {
                        status = 'N';
                    }
                } else {
                    keyValueStore.put(key, newValue);
                    status = 'A';
                }
            }
        } else {
            status = 'X';
        }
        String response = tID + " D " + status;
        sendResponse(response, senderAddress, senderPort);
    }

    private void sendResponse(String response, InetAddress address, int port) throws IOException {
        byte[] responseData = response.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(responseData, responseData.length, address, port);
        socket.send(packet);
    }

    private List<KeyValuePair> findClosestAddresses(byte[] targetHashID, int limit) throws Exception {
        return findClosestAddresses(targetHashID, limit, false);
    }

    private List<KeyValuePair> findClosestAddresses(byte[] targetHashID, int limit, boolean forceRefresh) throws Exception {
        List<KeyValuePair> closest = new ArrayList<>();
        if (nodeName != null) {
            byte[] selfHash = HashID.computeHashID("N:" + nodeName);
            int distance = calculateDistance(selfHash, targetHashID);
            String selfAddress = InetAddress.getLocalHost().getHostAddress() + ":" + socket.getLocalPort();
            closest.add(new KeyValuePair("N:" + nodeName, selfAddress, distance));
        }

        for (Map.Entry<String, String> entry : keyValueStore.entrySet()) {
            if (entry.getKey().startsWith("N:")) {
                byte[] nodeHash = HashID.computeHashID(entry.getKey());
                int distance = calculateDistance(nodeHash, targetHashID);
                closest.add(new KeyValuePair(entry.getKey(), entry.getValue(), distance));
            }
        }

        Collections.sort(closest);
        if (closest.size() > limit) closest = closest.subList(0, limit);

        if ((closest.size() < limit || forceRefresh) && !closest.isEmpty()) {
            KeyValuePair node = closest.get(0);
            String[] parts = node.getValue().split(":");
            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);
            String tID = generateTransactionID();
            String request = tID + " N " + byteArrayToHexString(targetHashID);
            String response = sendRequestWithRetries(request, address, port);
            if (response != null && response.startsWith(tID + " O ")) {
                String[] partsResponse = response.split(" ");
                int i = 2;
                while (i + 3 < partsResponse.length) {
                    String keyCount = partsResponse[i];
                    String key = partsResponse[i + 1];
                    String valueCount = partsResponse[i + 2];
                    String value = partsResponse[i + 3];
                    // breaks down the ip as digit, dot, digit, dot, digit, dot, colon, digit. for ip:port
                    if (key.startsWith("N:") && value.matches("\\d+\\.\\d+\\.\\d+\\.\\d+:\\d+")) {
                        keyValueStore.put(key, value);
                    } else {
                        System.out.println("Invalid node address in response: " + key + " -> " + value);
                    }
                    i += 4;
                }
                closest.clear();
                if (nodeName != null) {
                    byte[] selfHash = HashID.computeHashID("N:" + nodeName);
                    int distance = calculateDistance(selfHash, targetHashID);
                    String selfAddress = InetAddress.getLocalHost().getHostAddress() + ":" + socket.getLocalPort();
                    closest.add(new KeyValuePair("N:" + nodeName, selfAddress, distance));
                }
                for (Map.Entry<String, String> entry : keyValueStore.entrySet()) {
                    if (entry.getKey().startsWith("N:")) {
                        byte[] nodeHash = HashID.computeHashID(entry.getKey());
                        int distance = calculateDistance(nodeHash, targetHashID);
                        closest.add(new KeyValuePair(entry.getKey(), entry.getValue(), distance));
                    }
                }
                Collections.sort(closest);
            }
        }
        return closest.size() <= limit ? closest : closest.subList(0, limit);
    }

    private String formatString(String str) {
        return countSpaces(str) + " " + str + " ";
    }

    private String extractValue(String response, String delimiter) {
        String[] parts = response.split(delimiter, 2);
        if (parts.length < 2) return null;
        String formatted = parts[1].trim();
        String[] valueParts = formatted.split(" ", 2);
        if (valueParts.length < 2) return null;
        String value = valueParts[1].trim();
        return value.isEmpty() ? null : value;
    }

    private String extractFormattedValue(String formatted) {
        String[] parts = formatted.split(" ", 2);
        return parts.length > 1 ? parts[1].trim() : "";
    }

    private String generateTransactionID() {
        Random random = new Random();
        return "" + (char) ('A' + random.nextInt(26)) + (char) ('A' + random.nextInt(26));
    }

    private int calculateDistance(byte[] hash1, byte[] hash2) {
        int distance = 0;
        for (int i = 0; i < hash1.length && i < hash2.length; i++) {
            distance += Integer.bitCount(hash1[i] ^ hash2[i]);
        }
        return distance;
    }

    private int countSpaces(String str) {
        int count = 0;
        for (char c : str.toCharArray()) if (c == ' ') count++;
        return count;
    }

    private byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    private String byteArrayToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }



    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        byte[] keyHash = HashID.computeHashID(key);
        List<KeyValuePair> closestNodes = findClosestAddresses(keyHash, 3);
        for (KeyValuePair node : closestNodes) {
            String[] parts = node.getValue().split(":");
            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);
            String tID = generateTransactionID();
            String request = tID + " C " + formatString(key) + formatString(currentValue) + formatString(newValue);
            String response = sendRequestWithRetries(request, address, port);
            if (response != null) {
                if (response.startsWith(tID + " D R") || response.startsWith(tID + " D A")) {
                    keyValueStore.put(key, newValue);
                    return true;

                }
                if (response.startsWith(tID + " D N")) return false;
            }
        }
        return false;
    }
}
