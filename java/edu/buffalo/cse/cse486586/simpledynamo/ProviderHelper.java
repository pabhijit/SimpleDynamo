package edu.buffalo.cse.cse486586.simpledynamo;

import android.net.Uri;
import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Map;

/**
 * Created by abhijit on 4/20/15.
 * This class contains the reusable methods which are invoked from multiple points in the provider class.
 */
public class ProviderHelper {

    private static final String TAG = ProviderHelper.class.getSimpleName();

    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public static String searchCoordinator( String input ) {

        String coordinator = null;
        try {
            for( String key : SimpleDynamoProvider.hashedPorts.keySet() ){
                if( input.compareTo(key) < 0 ){
                    coordinator = SimpleDynamoProvider.hashedPorts.get(key);
                    break;
                }
                if(coordinator == null) {
                    coordinator = SimpleDynamoProvider.hashedPorts.firstEntry().getValue();
                }
            }
        } catch ( Exception e ) {
            Log.e(TAG, "SearchCoordinator: Exception message = " + e.getMessage());
        }
        return coordinator;
    }

    public static List<String> getPreferenceList(String input, boolean deadFlag) {

        List<String> retVal = new ArrayList<String>();
        try{
            input = ProviderHelper.genHash(String.valueOf(Integer.valueOf(input) / 2));
            int countMembers = 3;

            for( Map.Entry<String, String> entry : SimpleDynamoProvider.hashedPorts.entrySet() ){
                if( input.compareTo(entry.getKey()) <= 0 ) {
                    if( deadFlag || !isDead( entry.getValue() ) ) {
                        retVal.add(entry.getValue());
                    }
                    countMembers--;
                    if(countMembers == 0)
                        break;
                }
            }

            if( countMembers != 0 ){
                for( Map.Entry<String, String> entry : SimpleDynamoProvider.hashedPorts.entrySet() ){
                    if( deadFlag || !isDead( entry.getValue() ) ) {
                        retVal.add(entry.getValue());
                    }
                    countMembers--;
                    if(countMembers == 0)
                        break;

                }
            }

        } catch(Exception e){
            Log.e(TAG, "GetPreferenceList: Exception message = " + e.getMessage() );
        }
        return retVal;
    }

    public static boolean isDead( String input ) {

        return SimpleDynamoProvider.portStatus.get( input ).equals( DynamoConstants.STR_DEAD );
    }

    /*public static String getPredecessor(String str){
        try{
        String retVal = SimpleDynamoProvider.hashedPorts.lastEntry().getValue();
        for( Map.Entry<String, String> entry : SimpleDynamoProvider.hashedPorts.entrySet() ){
            if(str.equals(entry.getValue())){
                return retVal;
            }
            retVal = entry.getValue();

        }
    } catch(Exception e){
        Log.e(TAG, "GetPredecessor: Exception message = " + e.getMessage() );
    }
        return null;
    }
    public static String getSuccessor(String str){
        try{
        String retVal = SimpleDynamoProvider.hashedPorts.firstEntry().getValue();
        for( Map.Entry<String, String> entry : SimpleDynamoProvider.hashedPorts.descendingMap().entrySet() ){
            if(str.equals(entry.getValue())){
                return retVal;
            }
            retVal = entry.getValue();

        }
        } catch(Exception e){
            Log.e(TAG, "GetSuccessor: Exception message = " + e.getMessage() );
        }
        return null;
    }*/
}
