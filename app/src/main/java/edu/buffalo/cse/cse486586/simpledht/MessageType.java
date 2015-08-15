package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;

/**
 * Created by nikhil on 3/27/15.
 */
public enum MessageType implements Serializable {
    INIT{
        @Override
        public String toString() {
            return "INIT";
        }
    },
    INIT_REPLY{
        @Override
        public String toString() {
            return "INIT_REPLY";
        }
    },
    INSERT{
        @Override
        public String toString() {
            return "INSERT";
        }
    },
    QUERY{
        @Override
        public String toString() {
            return "QUERY";
        }
    },
    QUERY_REPLY{
        @Override
        public String toString() {
            return "QUERY_REPLY";
        }
    },
    QUERY_GLOBAL{
        @Override
        public String toString() {
            return "QUERY_GLOBAL";
        }
    },
    DELETE{
        @Override
        public String toString() {
            return "DELETE";
        }
    }

}