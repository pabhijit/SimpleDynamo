package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by abhijit on 4/20/15.
 * This class represents the client thread and is mainly used redirecting requests to the server thread and handling socket failures.
 */
public class ClientTask extends AsyncTask<String, Void, Void> {

    private static final String TAG = SimpleDynamoProvider.class.getSimpleName();

    @Override
    protected Void doInBackground( String... msgs ) {

        ObjectOutputStream writer;
        ObjectInputStream reader;
        Socket socket;
        String msgToSend = msgs[0];
        String splitMsg[] = null;
        try {
            splitMsg = msgToSend.split( "%%" );
            socket = new Socket( InetAddress.getByAddress( new byte[]{10, 0, 2, 2} ),
                    Integer.parseInt( msgs[1] ) );
            socket.setSoTimeout( 3000 );

            writer = new ObjectOutputStream( socket.getOutputStream() );
            writer.writeObject( new String( msgToSend ) );

            reader = new ObjectInputStream(socket.getInputStream());
            HashMap<String, String> map = (HashMap) reader.readObject();
            if (map != null) {
                if ( msgToSend.startsWith( DynamoConstants.STR_QUERY ) ) {
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        if (SimpleDynamoProvider.queryHelper.containsKey(entry.getKey())) {
                            String splitStrNew[] = entry.getValue().split("##");
                            String splitStrOld[] = SimpleDynamoProvider.queryHelper.get(entry.getKey()).split("##");
                            if (Long.valueOf(splitStrNew[0]) > Long.valueOf(splitStrOld[0])) {
                                SimpleDynamoProvider.queryHelper.put(entry.getKey(), entry.getValue());
                            }
                        } else {
                            SimpleDynamoProvider.queryHelper.put(entry.getKey(), entry.getValue());
                        }
                    }

                } else if ( msgToSend.startsWith( DynamoConstants.STR_ALIVE_MSG ) ) {
                    Log.e(TAG, "ClientTask: Processing missed messages : " + map.size());
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        SimpleDynamoProvider.recoveryMap.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            writer.close();
            reader.close();
            socket.close();
        } catch ( IOException ioe ) {
            Log.e( TAG, "ClientTask socket IOException" + msgs[0] );
            if(!msgs[0].contains(DynamoConstants.STR_ALIVE_MSG)) {
                processMissedMessage(msgs[0],msgs[1]);
                handleException(msgs[1]);
            }
        } catch ( Exception e) {
            Log.e( TAG, "ClientTask: Unknown Exception" + e.getMessage() );
        } finally{
            if (Integer.valueOf(splitMsg[2]) == 0) {
                Log.e(TAG, "ClientTask: Toggling onCreate wait flag");
                if(msgToSend.startsWith( DynamoConstants.STR_QUERY )) {
                    SimpleDynamoProvider.waitFlag = true;
                } else if(msgToSend.startsWith( DynamoConstants.STR_ALIVE_MSG )) {
                    SimpleDynamoProvider.createFlag = true;
                } else if(msgToSend.startsWith( DynamoConstants.STR_INSERT )) {
                    SimpleDynamoProvider.insertFlag = true;
                }
            }
        }
        return null;
    }

    public void handleException(String port){

        try {
            Log.e( TAG, "HandleException: Removing " + port );
            SimpleDynamoProvider.portStatus.put( port, DynamoConstants.STR_DEAD );
            for (String strPort : SimpleDynamoProvider.portStatus.keySet()) {
                if ( !ProviderHelper.isDead( strPort ) && !strPort.equals( port ) ) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(strPort));
                        socket.setSoTimeout( 3000 );
                        ObjectOutputStream writer = new ObjectOutputStream( socket.getOutputStream() );
                        writer.writeObject( DynamoConstants.STR_DEAD_MSG + "%%" + port );

                        ObjectInputStream reader = new ObjectInputStream(socket.getInputStream());
                        HashMap<String, String> map = (HashMap) reader.readObject();
                        writer.close();reader.close();
                        socket.close();
                    }catch (IOException e) {
                        Log.e(TAG, "ClientTask HandleException : IOException");
                    }catch (Exception e) {}
                }
            }
        } catch ( Exception e ) {
            Log.e(TAG, "ClientTask HandleException : Exception");
        }
    }

    public void processMissedMessage( String msg1, String msg2 ){
        try {
            if ( msg1.contains( DynamoConstants.STR_INSERT ) ) {
                String splitTemp[] = msg1.replace( DynamoConstants.STR_INSERT, "" ).split( "%%" );
                Log.d( TAG, "ProcessMissedMessage: " + msg2 + " missed : " + splitTemp[0] );
                Date date = new Date();
                if ( !SimpleDynamoProvider.deadMap.containsKey( msg2 ) ) {
                    HashMap<String, String> tempMap = new HashMap<>();
                    tempMap.put( splitTemp[0], String.valueOf( date.getTime() ) + "##" + splitTemp[1] );
                    SimpleDynamoProvider.deadMap.put(msg2, tempMap);
                } else {
                    SimpleDynamoProvider.deadMap.get( msg2 ).put( splitTemp[0], String.valueOf(date.getTime()) + "##" + splitTemp[1] );
                }
                Log.d(TAG, "Insert: Adding to deadMap " + SimpleDynamoProvider.deadMap.get(msg2).get(splitTemp[0]) );
            }
        }catch( Exception e ) {
            Log.e( TAG, "processMissedMessage Unknown Exception : " + e.getMessage() );
        }
    }
}