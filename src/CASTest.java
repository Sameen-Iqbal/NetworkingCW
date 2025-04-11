import java.net.InetAddress;

public class CASTest {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting CAS Test...");

        Node node1 = new Node();
        node1.setNodeName("N:node1");
        node1.openPort(20110);

        Node node2 = new Node();
        node2.setNodeName("N:node2");
        node2.openPort(20111);

        node1.write("N:node2", "127.0.0.1:20111");

        // Test 1: CAS on existing key
        node2.write("D:lock", "OldValue");
        node2.handleIncomingMessages(100);
        boolean casSuccess = node1.CAS("D:lock", "OldValue", "NewValue");
        node2.handleIncomingMessages(100);
        String result = node2.read("D:lock");
        if (casSuccess && "NewValue".equals(result)) {
            System.out.println("PASS: CAS replaced value: " + result);
        } else {
            System.out.println("FAIL: CAS replace failed, got: " + result);
        }

        // Test 2: CAS with wrong current value
        casSuccess = node1.CAS("D:lock", "WrongValue", "AnotherValue");
        node2.handleIncomingMessages(100);
        result = node2.read("D:lock");
        if (!casSuccess && "NewValue".equals(result)) {
            System.out.println("PASS: CAS failed with wrong value, kept: " + result);
        } else {
            System.out.println("FAIL: CAS with wrong value, got: " + result);
        }

        // Test 3: CAS on non-existent key
        casSuccess = node1.CAS("D:newlock", "None", "FirstValue");
        node2.handleIncomingMessages(100);
        result = node2.read("D:newlock");
        if (casSuccess && "FirstValue".equals(result)) {
            System.out.println("PASS: CAS added new key: " + result);
        } else {
            System.out.println("FAIL: CAS add failed, got: " + result);
        }

        System.out.println("CAS Test Completed.");
    }
}