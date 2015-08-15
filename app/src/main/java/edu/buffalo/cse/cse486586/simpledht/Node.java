package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by nikhil on 3/27/15.
 */
public class Node implements Serializable {
    public int port;
    public String ID;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node node = (Node) o;

        if (port != node.port) return false;
        if (!ID.equals(node.ID)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = port;
        result = 31 * result + ID.hashCode();
        return result;
    }

    public Node(int port, String ID) {
        this.port = port;
        this.ID = ID;
    }

    public static class NodeComparator implements Comparator<Node> {
        @Override
        public int compare(Node lhs, Node rhs) {
            if(lhs==null){
                return 1;
            } else if(rhs==null){
                return -1;
            }
            return lhs.ID.compareTo(rhs.ID);
        }
    }

    @Override
    public String toString() {
        return "Node{" +
                "port=" + port +
                /*", ID='" + ID + '\'' +*/
                '}';
    }
}
