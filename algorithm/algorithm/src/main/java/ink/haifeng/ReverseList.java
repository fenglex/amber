package ink.haifeng;

/**
 * 反转链表
 */
public class ReverseList {

    public static void main(String[] args) {
        Node node1 = new Node(1);
        Node node2 = new Node(2);
        Node node3 = new Node(3);
        Node node4 = new Node(4);
        Node node5 = new Node(5);
        Node node6 = new Node(6);
        Node node7 = new Node(7);
        Node node8 = new Node(8);
        node1.next = node3;
        node3.next = node5;
        node2.next = node4;
        node4.next = node6;
        node6.next = node7;
        node7.next = node8;

        Node node = mergeSortNode(node1, node2);
        System.out.println(node);


    }

    public static class Node {
        public int value;
        public Node next;

        @Override
        public String toString() {
            return value + "->" + next;
        }

        public Node(int value) {
            this.value = value;
        }
    }

    public static class DoubleNode {
        public int value;
        public DoubleNode pre;
        public DoubleNode next;

        public DoubleNode(int value, DoubleNode pre, DoubleNode next) {
            this.value = value;
            this.pre = pre;
            this.next = next;
        }
    }

    public static Node reverseLinkedList(Node head) {
        Node pre = null;
        Node next = null;
        while (head != null) {
            next = head.next;
            head.next = pre;
            pre = head;
            head = next;
        }
        return head;
    }

    public static DoubleNode reverseDoubleList(DoubleNode head) {
        DoubleNode pre = null;
        DoubleNode next = null;
        while (head != null) {
            next = head.next;
            head.next = pre;
            head.pre = next;
            pre = head;
            head = next;
        }
        return head;
    }


    public static Node mergeSortNode(Node head1, Node head2) {
        if (head1 == null || head2 == null) {
            return head1 == null ? head2 : head1;
        }
        Node cur = null;
        Node head = null;
        if (head1.value > head2.value) {
            cur = head2;
            head2 = cur.next;
        } else {
            cur = head1;
            head1 = cur.next;
        }
        head = cur;
        while (head1 != null && head2 != null) {
            if (head1.value > head2.value) {
                cur.next = head2;
                cur = cur.next;
                head2 = cur.next;
            } else {
                cur.next = head1;
                cur = cur.next;
                head1 = cur.next;
            }
        }
        cur.next = head1 != null ? head1 : head2;
        return head;
    }
}
