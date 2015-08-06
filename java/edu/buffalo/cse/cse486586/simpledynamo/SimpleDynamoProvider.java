package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    private final Uri providerUri = ProviderHelper.buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

    private static String[] cols = new String[] { DynamoConstants.KEY_FIELD, DynamoConstants.VALUE_FIELD };

    public static ConcurrentHashMap<String, String> portStatus = new ConcurrentHashMap<String, String>();
    public static ConcurrentHashMap<String, String> queryHelper = new ConcurrentHashMap<String, String>();
    public static ConcurrentSkipListMap<String, String> hashedPorts = new ConcurrentSkipListMap<String, String>();

    public static ConcurrentHashMap<String, HashMap<String,String>> deadMap = new ConcurrentHashMap<String, HashMap<String,String>>();
    public static ConcurrentHashMap<String, String> recoveryMap = new ConcurrentHashMap<String, String>();

    public static boolean waitFlag = false;
    public static boolean createFlag = false;
    public static boolean insertFlag = false;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        int retVal = 0;
        try {
            if (selection.matches("\"*\"")) {
                retVal = deleteAllPairs();
                if( selectionArgs == null ){
                    for (String entry : DynamoConstants.REMOTE_PORTS) {
                        if (!portStatus.get(entry).equals(DynamoConstants.STR_DEAD) && !entry.equals(getPortNumber())) {
                            new ClientTask().executeOnExecutor( AsyncTask.SERIAL_EXECUTOR, DynamoConstants.STR_DELETE + "%%" + selection + "%%0", entry );
                        }
                    }
                }
            } else {
                if (selection.matches("\"@\"")) {
                    retVal = deleteAllPairs();

                } else {
                    File file = new File(getContext().getFilesDir().getAbsolutePath() + "/" + selection);
                    if (file.exists()) {
                        file.delete();
                        retVal++;
                    }
                }
            }
            Log.e( TAG, "Number of files deleted is : " + retVal );
        }catch(Exception e){
            Log.e( TAG, "delete operation failed" );
        }
        return retVal;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String filename = null;
        String fileHashed = null;
        String content = null;

        String thisPort = getPortNumber();
        try {
            for( String key: values.keySet() ) {
                if( key.equals( "key" ) ){
                    filename = String.valueOf( values.get( key ) );
                    fileHashed = ProviderHelper.genHash( filename );
                } else {
                    content = String.valueOf( values.get( key ) );
                }
            }
            Log.d(TAG, "Insert: Key = " + filename );

            List<String> prefList = ProviderHelper.getPreferenceList(ProviderHelper.searchCoordinator(fileHashed), true);
            Log.d(TAG, "Insert: preference list size : " + prefList.size());
            if ( prefList.contains( thisPort ) ) {
                Date date = new Date();
                insertContent( filename, date.getTime() + "##" + content );
                prefList.remove( thisPort );
            }
            int countPref = 0;
            for( String pref : prefList ) {
                if(!ProviderHelper.isDead(pref)) {
                    countPref ++;
                }
            }
            for (String pref : prefList) {
                if (!ProviderHelper.isDead(pref)) {
                    Log.d(TAG, "Insert: inserting " + filename + " @ " + pref );
                    countPref--;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DynamoConstants.STR_INSERT + filename + "%%" + content + "%%" + countPref, pref);
                } else {
                    Log.d(TAG, "Insert: " + pref + " missed : " + filename);
                    Date date = new Date();
                    if (!deadMap.containsKey(pref)) {
                        HashMap<String, String> tempMap = new HashMap<>();
                        tempMap.put(filename, date.getTime() + "##" + content);
                        deadMap.put(pref, tempMap);
                    } else {
                        deadMap.get(pref).put(filename, date.getTime() + "##" + content);
                    }
                }
            }
            insertFlag = false;
            while (!insertFlag){

            }
            insertFlag = false;

        } catch ( Exception e ) {
            Log.e( TAG, "Insert: Exception : " + e.getMessage() );
        }

        Log.v("insert", values.toString());
        return uri;
    }

    @Override
    public boolean onCreate() {

        try {
            try {
                Log.d(TAG, "onCreate: Creating a ServerSocket");
                ServerSocket serverSocket = new ServerSocket(DynamoConstants.SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            } catch (IOException e) {
                Log.e(TAG, "onCreate: Can't create a ServerSocket");
                return false;
            }
            final String thisPort = getPortNumber();
            Thread createThread = new Thread() {
                @Override
                public void run() {

                    int iCount = 4;
                    for (String port : DynamoConstants.REMOTE_PORTS) {
                        try {
                            hashedPorts.put(ProviderHelper.genHash(String.valueOf(Integer.valueOf(port) / 2)), port);

                            portStatus.put(port, DynamoConstants.STR_ALIVE);
                            if (!port.equals(thisPort)) {
                                iCount = iCount - 1;
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DynamoConstants.STR_ALIVE_MSG + "%%" + thisPort + "%%" + String.valueOf(iCount), port);
                            }
                        } catch (Exception e) {
                            Log.e(TAG, "OnCreate: Exception while reporting aliveness " + e.getMessage());
                        }
                    }
                    createFlag = false;
                    while (!createFlag) {

                    }
                    createFlag = false;
                    Log.d(TAG, "onCreate: The size of the recovery map is " + recoveryMap.size());

                    for (Map.Entry<String, String> entry : recoveryMap.entrySet()) {
                        insertContent(entry.getKey(), entry.getValue());
                    }
                    recoveryMap = new ConcurrentHashMap<>();
                }
            };
            createThread.start();
        }catch(Exception e){
            Log.e(TAG, "onCreate:Exception in SimpleDhtProvider " + e.getMessage() );
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        String line;
        MatrixCursor cursor = new MatrixCursor(cols);
        String value = null;
        FileInputStream fis;
        BufferedReader reader;
        Log.d(TAG, "The selection parameter is : "+ selection);

        try {
            if( selection.contains("*") ){
                if(selectionArgs == null) {
                    synchronized ( this ) {
                        fetchAllFiles();
                        List<String> aliveNodes = new ArrayList<String>();
                        for (String port : portStatus.keySet()) {
                            if (portStatus.get(port).equals(DynamoConstants.STR_ALIVE)) {
                                aliveNodes.add(port);
                            }
                        }
                        aliveNodes.remove( getPortNumber() );
                        Log.d(TAG, "Number of nodes alive is " + aliveNodes.size());
                        int prefSize = aliveNodes.size();
                        for (String port : aliveNodes) {
                            prefSize = prefSize - 1;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DynamoConstants.STR_QUERY_GLOBAL + "%%" + selection + "%%" + String.valueOf(prefSize), port);
                        }
                        waitFlag = false;
                        while (!waitFlag) {

                        }
                        Log.d(TAG, "Global Query: Wait is over");
                        waitFlag = false;
                        for (String key : queryHelper.keySet()) {
                            String values[] = queryHelper.get(key).split("##");
                            values[0] = key;
                            cursor.addRow(values);
                        }
                        queryHelper = new ConcurrentHashMap<>();
                    }
                } else {
                    Log.d(TAG, "Replying the initiator ");
                    for (String file : getContext().fileList()) {
                        fis = getContext().openFileInput(file);
                        reader = new BufferedReader(new InputStreamReader(fis));

                        while ((line = reader.readLine()) != null) {
                            value = line;
                        }
                        String[] values = new String[]{file, value};
                        cursor.addRow(values);
                    }
                }
                value = null;
                Log.d( TAG, "Number of rows returned is : " + cursor.getCount() );
                return cursor;
            } else if( selection.matches( "@" ) ) {

                Log.d(TAG, "Replying the initiator ");
                for (String file : getContext().fileList()) {
                    fis = getContext().openFileInput(file);
                    reader = new BufferedReader(new InputStreamReader(fis));

                    while ((line = reader.readLine()) != null) {
                        String[] tempSplit = line.split("##");
                        value = tempSplit[1];
                    }
                    String[] values = new String[]{file, value};
                    cursor.addRow(values);
                }
                value = null;
                Log.d( TAG, "Number of rows returned is : " + cursor.getCount() );
                return cursor;
            } else {
                if(selectionArgs == null) {
                    synchronized ( this ) {
                        List<String> prefList = ProviderHelper.getPreferenceList(ProviderHelper.searchCoordinator(ProviderHelper.genHash(selection)), false );
                        Log.d(TAG, "Fetching the replicas from the preference list");
                        int prefSize = prefList.size();
                        for (String port : prefList) {
                            prefSize = prefSize - 1;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DynamoConstants.STR_QUERY_LOCAL + "%%" + selection + "%%" + String.valueOf(prefSize), port);
                        }
                        waitFlag = false;
                        while (!waitFlag) {

                        }
                        Log.d(TAG, "Local Query: Wait is over");
                        waitFlag = false;
                        if (queryHelper.get(selection) != null) {
                            String tempSplit[] = queryHelper.get(selection).split("##");
                            value = tempSplit[1];
                        }
                        queryHelper = new ConcurrentHashMap<>();
                        if( value != null ) {
                            String[] values = new String[]{selection, value};
                            cursor.addRow(values);
                        }
                        Log.d(TAG, "Query: Value returned returned is : " + value);
                    }
                } else {
                    Log.d(TAG, "Query: Replying the initiator ");
                    try {
                        fis = getContext().openFileInput(selection);
                        reader = new BufferedReader(new InputStreamReader(fis));
                        while ((line = reader.readLine()) != null) {
                            value = line;
                        }
                    } catch (Exception e) {
                        Log.e(TAG, "Query: File does not exist");
                    }
                    if( value != null ) {
                        String[] values = new String[]{selection, value};
                        cursor.addRow(values);
                    }
                }
                value = null;
                return cursor;
            }
        } catch (Exception e) {
            Log.e(TAG, "Query Exception" + e.getMessage());
            e.printStackTrace();
        }
        return cursor;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            HashMap<String,String> dummyMap = new HashMap<>();
            dummyMap.put( DynamoConstants.KEY_FIELD, DynamoConstants.VALUE_FIELD );
            String thisPort = getPortNumber();
            String msgReceived;
            Socket socket;
            try {
                while( true ) {
                    socket = serverSocket.accept();

                    ObjectInputStream reader = new ObjectInputStream(socket.getInputStream());
                    if ((msgReceived = (String)reader.readObject()) != null) {

                        msgReceived = msgReceived.trim();
                        ObjectOutputStream writer = new ObjectOutputStream( socket.getOutputStream() );

                        if( msgReceived.contains( DynamoConstants.STR_ALIVE_MSG ) ) {
                            String strReceiveSplit[] = msgReceived.split("%%");
                            Log.d( TAG, "ServerTask: "+strReceiveSplit[1]+" is reported Alive" );
                            if( portStatus.containsKey( strReceiveSplit[1] ) ) {
                                portStatus.put(strReceiveSplit[1], DynamoConstants.STR_ALIVE);
                            }
                            writer.writeObject(deadMap.get(strReceiveSplit[1]));
                            deadMap.remove(strReceiveSplit[1]);
                            writer.close();

                        } else if( msgReceived.contains( DynamoConstants.STR_DEAD_MSG ) ) {
                            String strReceiveSplit[] = msgReceived.split("%%");
                            Log.d( TAG, "ServerTask: "+strReceiveSplit[1]+" is reported Dead" );
                            if( portStatus.containsKey( strReceiveSplit[1] ) ) {
                                portStatus.put(strReceiveSplit[1], DynamoConstants.STR_DEAD);
                            }
                            writer.writeObject(dummyMap);

                        } else if(msgReceived.contains(DynamoConstants.STR_DELETE)){
                            String strReceiveSplit[] = msgReceived.split("%%");
                            Log.d( TAG, "ServerTask: Deleting selection : " + strReceiveSplit[1] );
                            getContext().getContentResolver().delete(providerUri, strReceiveSplit[1], strReceiveSplit);
                            writer.writeObject(dummyMap);

                        } else if(msgReceived.contains(DynamoConstants.STR_QUERY_GLOBAL) ){
                            String strReceiveSplit[] = msgReceived.split("%%");
                            String sendSelectArgs[] = new String[]{strReceiveSplit[1]};
                            Cursor resultCursor = getContext().getContentResolver().query(providerUri, null, strReceiveSplit[1], sendSelectArgs, null);
                            HashMap<String, String> returnValue = new HashMap<String, String>();
                            try {
                                if (resultCursor != null) {
                                    Log.d(TAG, "ServerTask: GQuerying @ " + thisPort + " returned " + resultCursor.getCount());
                                    int keyIndex = resultCursor.getColumnIndex(DynamoConstants.KEY_FIELD);
                                    int valueIndex = resultCursor.getColumnIndex(DynamoConstants.VALUE_FIELD);

                                    for (resultCursor.moveToFirst(); !resultCursor.isAfterLast(); resultCursor.moveToNext()) {
                                        returnValue.put(resultCursor.getString(keyIndex), resultCursor.getString(valueIndex));
                                    }
                                    resultCursor.close();
                                }
                            }catch( Exception e ){}
                            writer.writeObject(returnValue);
                        } else if(msgReceived.contains(DynamoConstants.STR_QUERY_LOCAL)){
                            Log.d( TAG, "ServerTask: LQuerying @ " + thisPort );
                            String strReceiveSplit[] = msgReceived.split("%%");
                            String sendSelectArgs[] = new String[]{strReceiveSplit[1]};
                            HashMap<String, String> returnValue = new HashMap<String, String>();
                            try {
                                Cursor resultCursor = getContext().getContentResolver().query(providerUri, null, strReceiveSplit[1], sendSelectArgs, null);
                                if (resultCursor != null) {
                                    int valueIndex = resultCursor.getColumnIndex(DynamoConstants.VALUE_FIELD);
                                    if (valueIndex != -1) {
                                        resultCursor.moveToFirst();
                                        returnValue.put(strReceiveSplit[1], resultCursor.getString(valueIndex));
                                        Log.d(TAG, "ServerTask: The value returned is : " + returnValue.get(strReceiveSplit[1]));
                                    }
                                    resultCursor.close();
                                }
                            }catch( Exception e ){}
                            writer.writeObject(returnValue);
                        } else if(msgReceived.contains(DynamoConstants.STR_INSERT)){
                            Log.d(TAG, "ServerTask: Inserting @ " + thisPort);
                            msgReceived = msgReceived.replace(DynamoConstants.STR_INSERT,"");
                            StringTokenizer tokenizer = new StringTokenizer(msgReceived,"%%");
                            Date date = new Date();
                            insertContent( tokenizer.nextToken(), date.getTime() + "##" + tokenizer.nextToken() );

                            writer.writeObject(dummyMap);
                        }
                        writer.close();
                    }
                    reader.close();
                }
            }catch (IOException ioe){
                Log.e(TAG, "ServerTask socket IOException" );
                ioe.printStackTrace();
            }catch(Exception e){
                Log.e(TAG, "ServerTask Exception Unknown"+ e.getMessage());
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {

        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String getPortNumber() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return String.valueOf((Integer.parseInt(portStr) * 2));
    }

    private int deleteAllPairs() {

        int retVal = 0;
        try{
            Log.d(TAG, "Deleting the local files");
            File file = new File(getContext().getFilesDir().getAbsolutePath());

            for(File toBeDeleted : file.listFiles()){
                toBeDeleted.delete();
                retVal += 1;
            }

        }catch(Exception e){
            Log.e(TAG, "DeleteAllPairs: Exception message = " + e.getMessage() );
        }
        return retVal;
    }

    public void insertContent(String filename, String content){

        FileOutputStream outputStream;
        try {
            outputStream = getContext().openFileOutput( filename, Context.MODE_PRIVATE );
            outputStream.write(content.getBytes());
            outputStream.close();
            Log.d(TAG,"InsertContent: Insert new value : "+ content);
        }catch(Exception e){
            Log.e(TAG, "InsertContent: Exception" );
        }
    }

    private void fetchAllFiles() {

        String line;
        FileInputStream fis;
        BufferedReader reader;
        try{
            Log.d(TAG, "FetchAllFiles: Fetching now");
            for (String file : getContext().fileList()) {
                fis = getContext().openFileInput(file);
                reader = new BufferedReader(new InputStreamReader(fis));

                while ((line = reader.readLine()) != null) {
                    queryHelper.put(file, line);
                }
            }
        }catch( Exception e ){
            Log.e(TAG, "FetchAllFiles: Exception");
        }
    }
}