package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by abhijit on 4/19/15.
 */
public class DynamoConstants {

    public static final int SERVER_PORT = 10000;
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";
    public static final String STR_DELETE = "delete";
    public static final String STR_QUERY_GLOBAL = "queryGlobal";
    public static final String STR_QUERY_LOCAL = "queryLocal";
    public static final String STR_QUERY = "query";
    public static final String STR_INSERT = "insert";
    public static final String STR_ALIVE = "alive";
    public static final String STR_DEAD = "dead";
    public static final String STR_DEAD_MSG ="reportDead";
    public static final String STR_ALIVE_MSG ="reportAlive";

    public static List<String> REMOTE_PORTS = new ArrayList<String>(){{
        add("11108");add("11112");add("11116");add("11120");add("11124");
    }};
}
