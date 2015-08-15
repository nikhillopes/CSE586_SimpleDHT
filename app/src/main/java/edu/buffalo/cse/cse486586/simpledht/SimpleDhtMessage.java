package edu.buffalo.cse.cse486586.simpledht;

import android.database.Cursor;
import android.database.MatrixCursor;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by nikhil on 3/27/15.
 */
public class SimpleDhtMessage implements Serializable {

    private MessageType messageType;
    private Node successorNode;
    private Node predecessorNode;
    private Node sourceNode;
    private Node destinationNode;
    private String key;
    private String value;
    private String selection;
    private HashMap<String, String> queryResult;

    public HashMap<String, String> getQueryResult() {
        return queryResult;
    }

    public void setQueryResult(HashMap<String, String> queryResult) {
        this.queryResult = queryResult;
    }

    public SimpleDhtMessage(MessageType messageType) {
        this.messageType = messageType;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public Node getSuccessorNode() {
        return successorNode;
    }

    public void setSuccessorNode(Node successorNode) {
        this.successorNode = successorNode;
    }

    public Node getPredecessorNode() {
        return predecessorNode;
    }

    public void setPredecessorNode(Node predecessorNode) {
        this.predecessorNode = predecessorNode;
    }

    public Node getSourceNode() {
        return sourceNode;
    }

    public void setSourceNode(Node sourceNode) {
        this.sourceNode = sourceNode;
    }

    public Node getDestinationNode() {
        return destinationNode;
    }

    public void setDestinationNode(Node destinationNode) {
        this.destinationNode = destinationNode;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getSelection() {
        return selection;
    }

    public void setSelection(String selection) {
        this.selection = selection;
    }

    @Override
    public String toString() {
        return "Message{" +
                "mT=" + messageType +
                ", souN=" + sourceNode +
                ", preN=" + predecessorNode +
                ", desN=" + destinationNode +
                ", sucN=" + successorNode +
                ", key='" + key + '\'' +
                ", val='" + value + '\'' +
                ", sel='" + selection + '\'' +
                ", QRes=" + queryResult +
                '}';
    }
}
