import java.net.*;

public class RobustnessTest {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting Robustness Test...");

        Node node = new Node();
        node.setNodeName("N:test");
        node.openPort(20110);

        InetAddress localhost = InetAddress.getByName("127.0.0.1");

        // Test 1: Malformed message (too short)
        node.receiveMessage("A ", localhost, 20111);
        System.out.println("PASS: No crash on short message");

        // Test 2: Invalid message type
        node.receiveMessage("BB Z foo", localhost, 20111);
        System.out.println("PASS: No crash on unknown message type");

        // Test 3: Malformed relay
        String badRelay = "CC V N:bad ";
        DatagramSocket tempSocket = new DatagramSocket();
        byte[] data = badRelay.getBytes("UTF-8");
        DatagramPacket packet = new DatagramPacket(data, data.length, localhost, 20110);
        tempSocket.send(packet);
        tempSocket.close();
        node.handleIncomingMessages(100);
        System.out.println("PASS: No crash on malformed relay");

        // Test 4: Malformed O response
        String badNearest = "DD O 0 N:foo 1 bad ip ";
        node.receiveMessage(badNearest, localhost, 20111);
        // Check if "bad ip" was stored by trying to read it
        String result = node.read("N:foo");
        if (result == null || !"bad ip".equals(result)) {
            System.out.println("PASS: Rejected invalid O response");
        } else {
            System.out.println("FAIL: Accepted invalid O response, got: " + result);
        }

        // Test 5: Overflow write
        String longValue = "x".repeat(1000);
        node.write("D:overflow", longValue);
        result = node.read("D:overflow");
        if (longValue.equals(result)) {
            System.out.println("PASS: Handles large value");
        } else {
            System.out.println("FAIL: Large value handling, got: " + result);
        }

        System.out.println("Robustness Test Completed.");
    }
}