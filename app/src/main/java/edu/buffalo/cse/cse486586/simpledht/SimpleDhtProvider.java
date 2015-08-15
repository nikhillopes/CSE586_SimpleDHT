package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Semaphore;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000, DHT_Max_Nodes = 10;
    static final String selectAllGlobal = "\"*\"";
    static final String selectAllLocal = "\"@\"";
    private static final String dbName = "keyValue.db";
    private final Semaphore queryResultAvailable = new Semaphore(1, true);
    HashMap<String, String> queryResult=null;
    ArrayList<Node> nodeArrayList = null;
    Node me, successor, predecessor, introducer;
    boolean iAmIntroducer = false, gotMessages = false, iAmAlone=true;
    private MainDatabaseHelper mOpenHelper;
    private SQLiteDatabase db;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.v(TAG, "deleteRequest Key = " + selection);
        String sqlLiteSelection;
        if (selection.equals(selectAllGlobal) || selection.equals(selectAllLocal)) {
            sqlLiteSelection = null;
        } else {
            sqlLiteSelection = " key = '" + selection + "'";
        }
        Log.v(TAG, "query=" + sqlLiteSelection);
        if(iAmAlone){
            try {
                db.delete("chats" , sqlLiteSelection, null);
            } catch (Exception e) {
                Log.e(TAG, "delete Error");
            }
            return 0;
        } else {
            if (sqlLiteSelection!=null && isInMyRange(selection) == true) {
                try {
                    db.delete("chats" , sqlLiteSelection, null);
                } catch (Exception e) {
                    Log.e(TAG, "query Error");
                }
                return 0;
            } else if (sqlLiteSelection==null && selection.equals(selectAllLocal)) {
                try {
                    db.delete("chats" , sqlLiteSelection, null);
                } catch (Exception e) {
                    Log.e(TAG, "query Error");
                }
                return 0;
            } else if (sqlLiteSelection==null && selection.equals(selectAllGlobal)) {
                try {
                    db.delete("chats" , sqlLiteSelection, null);
                } catch (Exception e) {
                    Log.e(TAG, "query Error");
                }
                SimpleDhtMessage delete =new SimpleDhtMessage(MessageType.DELETE);
                delete.setSourceNode(me);
                delete.setDestinationNode(successor);
                delete.setSelection(selection);
                new sendTo().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, delete);
                return 0;
            } else if(sqlLiteSelection!=null && isInMyRange(selection) == false) {
                SimpleDhtMessage delete =new SimpleDhtMessage(MessageType.DELETE);
                delete.setSourceNode(me);
                delete.setDestinationNode(successor);
                delete.setSelection(selection);
                new sendTo().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, delete);
                return 0;
            }
        }
        return -1;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        Log.v(TAG, "insertRequest Key = " + values.getAsString("key") + " || Value " + values.getAsString("value"));

        if (isInMyRange(values.getAsString("key"))) {
            long resultOfQuery = 0;
            db = mOpenHelper.getWritableDatabase();
            gotMessages = true;
            resultOfQuery = db.insertWithOnConflict("chats", null, values, SQLiteDatabase.CONFLICT_REPLACE);
            if (resultOfQuery == -1) {
                Log.v(TAG, "Insert Failed");
            } else {
                Log.v(TAG, "Insert Success");
            }

            Log.v(TAG, "insertedKey = " + values.getAsString("key") + " || insertedValue " + values.getAsString("value"));
            return uri;
        } else {
            SimpleDhtMessage insert = new SimpleDhtMessage(MessageType.INSERT);
            insert.setKey(values.getAsString("key"));
            insert.setValue(values.getAsString("value"));
            insert.setSourceNode(me);
            insert.setDestinationNode(successor);
            new sendTo().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert);
            return uri;
        }
    }

    @Override
    public boolean onCreate() {
        mOpenHelper = new MainDatabaseHelper(getContext());

        Server MyServer = new Server();
        Thread serverThread = new Thread(MyServer);
        serverThread.start();
        Log.v(TAG, "Started server");

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        int myPortInt = Integer.parseInt(portStr);
        try {
            if (myPortInt == 5554) {
                me = new Node(myPortInt, genHash(Integer.toString(myPortInt)));
                introducer = new Node(me.port, me.ID);
                predecessor = new Node(me.port, me.ID);
                successor = new Node(me.port, me.ID);
                iAmIntroducer = true;
                nodeArrayList = new ArrayList<>(DHT_Max_Nodes);
                nodeArrayList.add(me);
                Log.v(TAG, "I = " + me.toString() + " Introducer = " + introducer.toString());
            } else {
                me = new Node(myPortInt, genHash(Integer.toString(myPortInt)));
                predecessor = new Node(me.port, me.ID);
                successor = new Node(me.port, me.ID);
                introducer = new Node(5554, genHash(Integer.toString(5554)));
                Log.v(TAG, "I = " + me.toString() + " Introducer = " + introducer.toString());
                SimpleDhtMessage init_message = new SimpleDhtMessage(MessageType.INIT);
                init_message.setSourceNode(me);
                init_message.setDestinationNode(introducer);
                new sendTo().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, init_message);
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "" + e);
        }
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        Log.v(TAG, "queryRequest Key = " + selection);
        String columns[] = {"key", "value"};
        Cursor cursorToReturn = null;
        String sqlLiteSelection;
        if (selection.equals(selectAllGlobal) || selection.equals(selectAllLocal)) {
            sqlLiteSelection = null;
        } else {
            sqlLiteSelection = " key = '" + selection + "'";
        }
        Log.v(TAG, "query=" + sqlLiteSelection);
        if(iAmAlone){
            try {
                cursorToReturn = db.query("chats", columns, sqlLiteSelection, null, null, null, null);
            } catch (Exception e) {
                Log.e(TAG, "query Error");
            }
            return cursorToReturn;
        } else {
            if (sqlLiteSelection!=null && isInMyRange(selection) == true) {
                try {
                    cursorToReturn = db.query("chats", columns, sqlLiteSelection, null, null, null, null);
                } catch (Exception e) {
                    Log.e(TAG, "query Error");
                }
                return cursorToReturn;
            } else if (sqlLiteSelection==null && selection.equals(selectAllLocal)) {
                try {
                    cursorToReturn = db.query("chats", columns, sqlLiteSelection, null, null, null, null);
                } catch (Exception e) {
                    Log.e(TAG, "query Error");
                }
                return cursorToReturn;
            } else if (sqlLiteSelection==null && selection.equals(selectAllGlobal)) {
                try {
                    cursorToReturn = db.query("chats", columns, sqlLiteSelection, null, null, null, null);
                } catch (Exception e) {
                    Log.e(TAG, "query Error");
                }

                queryResultAvailable.acquireUninterruptibly();
                queryResult=null;
                SimpleDhtMessage query = new SimpleDhtMessage(MessageType.QUERY_GLOBAL);
                HashMap<String, String> temp=new HashMap<>();
                if(cursorToReturn!=null && cursorToReturn.moveToFirst()){
                    do {
                        temp.put(cursorToReturn.getString(cursorToReturn.getColumnIndex("key")),cursorToReturn.getString(cursorToReturn.getColumnIndex("value")));
                    } while(cursorToReturn.moveToNext());
                }
                query.setQueryResult(temp);
                query.setSourceNode(me);
                query.setSelection(selection);
                query.setDestinationNode(successor);

                new sendTo().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query);
                queryResultAvailable.acquireUninterruptibly();
                queryResultAvailable.release();
                Log.v(TAG, queryResult.toString());
                MatrixCursor returnCursor = new MatrixCursor(new String[] {"key", "value"});
                String keys[]=new String[queryResult.size()];
                queryResult.keySet().toArray(keys);
                for(int i=0; i < keys.length; i++){
                    returnCursor.newRow().add("key", keys[i]).add("value", queryResult.get(keys[i]));
                }
                return returnCursor;
            } else if(sqlLiteSelection!=null && isInMyRange(selection) == false) {
                queryResultAvailable.acquireUninterruptibly();
                queryResult=null;
                SimpleDhtMessage query = new SimpleDhtMessage(MessageType.QUERY);
                query.setSourceNode(me);
                query.setSelection(selection);
                query.setDestinationNode(successor);
                new sendTo().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query);
                queryResultAvailable.acquireUninterruptibly();
                queryResultAvailable.release();
                Log.v(TAG, queryResult.toString());
                MatrixCursor returnCursor = new MatrixCursor(new String[] {"key", "value"});
                String keys[]=new String[queryResult.size()];
                queryResult.keySet().toArray(keys);
                for(int i=0; i < keys.length; i++){
                    returnCursor.newRow().add("key", keys[i]).add("value", queryResult.get(keys[i]));
                }
                return returnCursor;
            }
            return null;
        }
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private boolean isInMyRange(String input) {
        if(iAmAlone){
            return true;
        }
        try {
            String hashedInput = genHash(input);
            if (hashedInput.compareTo(predecessor.ID) > 0 && hashedInput.compareTo(me.ID) <= 0) {
                Log.v(TAG,"isInMyRange = " + hashedInput +" pre = "+ predecessor.ID + " me = "+ me.ID);
                return true;
            } else if(predecessor.ID.compareTo(me.ID) > 0 && hashedInput.compareTo(predecessor.ID) > 0 && hashedInput.compareTo(me.ID) > 0){
                Log.v(TAG,"isInMyRange = " + hashedInput +" pre = "+ predecessor.ID + " me = "+ me.ID);
                return true;
            } else if(predecessor.ID.compareTo(me.ID) > 0 && hashedInput.compareTo(predecessor.ID) < 0 && hashedInput.compareTo(me.ID) <= 0){
                Log.v(TAG,"isInMyRange = " + hashedInput +" pre = "+ predecessor.ID + " me = "+ me.ID);
                return true;
            } else {
                Log.v(TAG,"isNotInMyRange = " + hashedInput +" pre = "+ predecessor.ID + " me = "+ me.ID);
                return false;
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "" + e);
        }
        return false;
    }

    protected static final class MainDatabaseHelper extends SQLiteOpenHelper {

        private static final String SQL_CREATE_MAIN = "CREATE TABLE chats (_ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE, key TEXT UNIQUE, value TEXT)";

        MainDatabaseHelper(Context context) {
            super(context, dbName, null, 1);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(SQL_CREATE_MAIN);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            Log.e(TAG, "XXX");
        }
    }

    private class sendTo extends AsyncTask<SimpleDhtMessage, Void, Void> {

        @Override
        protected Void doInBackground(SimpleDhtMessage... params) {
            SimpleDhtMessage messageToSend = params[0];
            try {
                Socket senderSocket;
                senderSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (messageToSend.getDestinationNode().port * 2));
                Log.v(TAG, "Sending To = " + messageToSend.getDestinationNode().port * 2);
                ObjectOutputStream out = new ObjectOutputStream(senderSocket.getOutputStream());
                out.writeObject(messageToSend);
                out.flush();
                out.close();
                senderSocket.close();
                //Log.v(TAG, "Snt" + messageToSend.toString());
            } catch (UnknownHostException f) {
                Log.e(TAG, "ClientTask UnknownHostException", f);
            } catch (IOException f) {
                Log.e(TAG, "ClientTask socket IOException", f);
            }
            return null;
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private class Server implements Runnable {
        @Override
        public void run() {

            try {
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

                while (true) {

                    Socket inSocket = serverSocket.accept();
                    ObjectInputStream in = new ObjectInputStream(inSocket.getInputStream());
                    SimpleDhtMessage receivedMessage = (SimpleDhtMessage) in.readObject();
                    Log.v(TAG, "START........................................................");
                    Log.v(TAG, "got" + receivedMessage.toString());

                    if (receivedMessage.getMessageType() == MessageType.INIT && iAmIntroducer == true) {

                        nodeArrayList.add(receivedMessage.getSourceNode());
                        Collections.sort(nodeArrayList, new Node.NodeComparator());
                        Log.v(TAG, nodeArrayList.toString());
                        Node temp[] = new Node[nodeArrayList.size()];
                        nodeArrayList.toArray(temp);

                        for (int i = 0; i < temp.length; i++) {
                            SimpleDhtMessage init_reply = new SimpleDhtMessage(MessageType.INIT_REPLY);
                            init_reply.setSourceNode(me);
                            init_reply.setDestinationNode(temp[i]);
                            if (i == 0) {
                                init_reply.setPredecessorNode(temp[temp.length - 1]);
                            } else {
                                init_reply.setPredecessorNode(temp[i - 1]);
                            }
                            if (i == temp.length - 1) {
                                init_reply.setSuccessorNode(temp[0]);
                            } else {
                                init_reply.setSuccessorNode(temp[i + 1]);
                            }
                            Log.v(TAG, "Snt" + init_reply.toString());
                            new sendTo().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, init_reply);
                        }
                        Log.v(TAG, "END........................................................");
                    } else if (receivedMessage.getMessageType() == MessageType.INIT_REPLY && receivedMessage.getSourceNode().equals(introducer) && receivedMessage.getDestinationNode().equals(me)) {
                        iAmAlone=false;
                        if (gotMessages == false) {
                            Log.v(TAG, "Handling InitJoin");
                            Log.v(TAG, "old pre = " +predecessor.toString() +" old suc ="+successor.toString());
                            successor = new Node(receivedMessage.getSuccessorNode().port, receivedMessage.getSuccessorNode().ID);
                            predecessor = new Node(receivedMessage.getPredecessorNode().port, receivedMessage.getPredecessorNode().ID);
                            Log.v(TAG, "new pre = " + predecessor.toString() + " new suc =" + successor.toString());
                        } else if (gotMessages == true && !predecessor.equals(receivedMessage.getPredecessorNode())) {
                            Log.v(TAG, "Handling LateJoin, Messages Rearranging Required");
                            Log.v(TAG, "old pre = " +predecessor.toString() +" old suc ="+successor.toString());
                            successor = new Node(receivedMessage.getSuccessorNode().port, receivedMessage.getSuccessorNode().ID);
                            predecessor = new Node(receivedMessage.getPredecessorNode().port, receivedMessage.getPredecessorNode().ID);
                            Log.v(TAG, "new pre = " +predecessor.toString() +" new suc ="+successor.toString());
                        } else if (gotMessages == true && !successor.equals(receivedMessage.getSuccessorNode())) {
                            Log.v(TAG, "Handling LateJoin, Messages Rearranging Not Required");
                            Log.v(TAG, "old pre = " +predecessor.toString() +" old suc ="+successor.toString());
                            successor = new Node(receivedMessage.getSuccessorNode().port, receivedMessage.getSuccessorNode().ID);
                            predecessor = new Node(receivedMessage.getPredecessorNode().port, receivedMessage.getPredecessorNode().ID);
                            Log.v(TAG, "new pre = " +predecessor.toString() +" new suc ="+successor.toString());
                        }
                        Log.v(TAG, "END........................................................");
                    } else if(receivedMessage.getMessageType() == MessageType.INSERT && receivedMessage.getDestinationNode().equals(me)){
                        if(isInMyRange(receivedMessage.getKey())){
                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            ContentValues mContentValues = new ContentValues();

                            mContentValues.put("key", receivedMessage.getKey());
                            mContentValues.put("value", receivedMessage.getValue());
                            getContext().getContentResolver().insert(mUri, mContentValues);
                        } else {
                            receivedMessage.setDestinationNode(successor);
                            new sendTo().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, receivedMessage);
                        }
                        Log.v(TAG, "END........................................................");
                    } else if(receivedMessage.getMessageType() == MessageType.DELETE && receivedMessage.getDestinationNode().equals(me)){
                        if(receivedMessage.getSourceNode().equals(me)){
                            Log.v(TAG, "Global Delete complete");
                            in.close();
                            inSocket.close();
                            continue;
                        }
                        if(receivedMessage.getSelection().equals(selectAllGlobal)) {
                            Log.v(TAG, "is a global delete, performing local delete and forwarding");
                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            getContext().getContentResolver().delete(mUri, selectAllLocal, null);
                            receivedMessage.setDestinationNode(successor);
                            new sendTo().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, receivedMessage);
                        } else if (isInMyRange(receivedMessage.getSelection()) == true) {
                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            getContext().getContentResolver().delete(mUri, receivedMessage.getSelection(), null);
                        } else if (isInMyRange(receivedMessage.getSelection()) == false) {
                            receivedMessage.setDestinationNode(successor);
                            new sendTo().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, receivedMessage);
                        }
                        Log.v(TAG, "END........................................................");
                    } else if(receivedMessage.getMessageType() == MessageType.QUERY_GLOBAL && receivedMessage.getDestinationNode().equals(me)){
                        if(receivedMessage.getSourceNode().equals(me)){
                            Log.v(TAG, "Global Query complete");
                            queryResult=(HashMap)receivedMessage.getQueryResult().clone();
                            queryResultAvailable.release();
                            in.close();
                            inSocket.close();
                            continue;
                        }
                        if(receivedMessage.getSelection().equals(selectAllGlobal)) {
                            Log.v(TAG, "is a global query, performing local query and forwarding");
                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            HashMap<String,String> temp=receivedMessage.getQueryResult();
                            Log.v(TAG,"HashMap of query before my entry= "+temp.toString());
                            Cursor tempCursor=getContext().getContentResolver().query(mUri,null, selectAllLocal, null, null);
                            if(tempCursor!=null && tempCursor.moveToFirst()){
                                do {
                                    temp.put(tempCursor.getString(tempCursor.getColumnIndex("key")),tempCursor.getString(tempCursor.getColumnIndex("value")));
                                } while(tempCursor.moveToNext());
                            }
                            Log.v(TAG,"HashMap of query after my entry= "+temp.toString());
                            receivedMessage.setQueryResult(temp);
                            receivedMessage.setDestinationNode(successor);
                            new sendTo().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, receivedMessage);
                        }
                        Log.v(TAG, "END........................................................");
                    } else if(receivedMessage.getMessageType() == MessageType.QUERY && receivedMessage.getDestinationNode().equals(me)){
                        if (isInMyRange(receivedMessage.getSelection()) == true) {
                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            HashMap<String,String> temp=new HashMap<>();
                            Cursor tempCursor = getContext().getContentResolver().query(mUri, null, receivedMessage.getSelection(), null, null);
                            if(tempCursor!=null && tempCursor.moveToFirst()){
                                do {
                                    temp.put(tempCursor.getString(tempCursor.getColumnIndex("key")),tempCursor.getString(tempCursor.getColumnIndex("value")));
                                } while(tempCursor.moveToNext());
                            }
                            Log.v(TAG,"HashMap of query= "+temp.toString());
                            SimpleDhtMessage query_reply = new SimpleDhtMessage(MessageType.QUERY_REPLY);
                            query_reply.setQueryResult(temp);
                            query_reply.setSourceNode(me);
                            query_reply.setDestinationNode(receivedMessage.getSourceNode());
                            Log.v(TAG, "QueryReply= " +query_reply.toString());
                            new sendTo().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query_reply);
                        } else if (isInMyRange(receivedMessage.getSelection()) == false) {
                            receivedMessage.setDestinationNode(successor);
                            new sendTo().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, receivedMessage);
                        }
                        Log.v(TAG, "END........................................................");
                    } else if(receivedMessage.getMessageType() == MessageType.QUERY_REPLY && receivedMessage.getDestinationNode().equals(me)){
                        Log.v(TAG, "Query complete");
                        queryResult=(HashMap)receivedMessage.getQueryResult().clone();
                        queryResultAvailable.release();
                        Log.v(TAG, "END........................................................");
                    }
                    in.close();
                    inSocket.close();
                }
            } catch (IOException e) {
                Log.e(TAG, "Can't accept connection", e);
            } catch (ClassNotFoundException e) {
                Log.e(TAG, "Class Not Fount Exeception", e);
            }
        }
    } //Server End
}
