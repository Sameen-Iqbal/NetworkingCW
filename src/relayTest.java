import java.net.*;

public class relayTest {
    // test case for the relay
    public static void main(String[] args) throws Exception {
        System.out.println("Starting Relay Test...");

        Node requester = new Node();
        requester.setNodeName("N:requester");
        requester.openPort(20110);

        Node relay = new Node();
        relay.setNodeName("N:relay");
        relay.openPort(20111);

        Node target = new Node();
        target.setNodeName("N:target");
        target.openPort(20112);

        String vmIP = "10.216.34.173";

        // Bootstrap
        relay.write("N:target", vmIP + ":20112");
        target.handleIncomingMessages(100);
        requester.write("N:relay", vmIP + ":20111");
        relay.handleIncomingMessages(100);

        // Test 1: Relay a Read Request
        target.write("D:secret", "Hidden Message");
        target.handleIncomingMessages(100);

        String tID = "RR";
        String innerMsg = "TT R 0 D:secret ";
        String relayMsg = tID + " V N:target " + innerMsg;
        DatagramSocket tempSocket = new DatagramSocket();
        byte[] data = relayMsg.getBytes("UTF-8");
        DatagramPacket packet = new DatagramPacket(data, data.length, InetAddress.getByName(vmIP), 20111);
        tempSocket.send(packet);
        tempSocket.close();

        Thread.sleep(100);
        relay.handleIncomingMessages(500);
        target.handleIncomingMessages(500);
        requester.handleIncomingMessages(500);

        String result = requester.read("D:secret");
        System.out.println("Read result: " + result);
        if ("Hidden Message".equals(result)) {
            System.out.println("PASS: Relay read request succeeded: " + result);
        } else {
            System.out.println("FAIL: Relay read failed, got: " + result);
        }

        // Test 2: Relay a Write Request
        String writeTID = "WW";
        String writeInner = "UU W 0 D:secret 1 New Value ";
        String writeRelayMsg = writeTID + " V N:target " + writeInner;
        tempSocket = new DatagramSocket();
        data = writeRelayMsg.getBytes("UTF-8");
        packet = new DatagramPacket(data, data.length, InetAddress.getByName(vmIP), 20111);
        tempSocket.send(packet);
        tempSocket.close();

        Thread.sleep(100);
        relay.handleIncomingMessages(500);
        target.handleIncomingMessages(500);
        requester.handleIncomingMessages(500);

        result = target.read("D:secret");
        System.out.println("Write result: " + result);
        if ("New Value".equals(result)) {
            System.out.println("PASS: Relay write request succeeded: " + result);
        } else {
            System.out.println("FAIL: Relay write failed, got: " + result);
        }

        System.out.println("Relay Test Completed.");
    }
}