package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    ArrayList<String> keys = new ArrayList<String>();
    static final String TAG = "shahyash";
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static String minePort;
    String meraLambaPort;
    ArrayList<String> hashedPortAlive = new ArrayList<String>();
    ArrayList<String> portAlive = new ArrayList<String>(); // list of alive ports
    static HashMap<String, String> portMap = new HashMap<String, String>();
    String mineHashedPort;

    public boolean isThisMyNode(String key) {
        String hashedKey = "";
        try {
            hashedKey = genHash(key);
        }catch (NoSuchAlgorithmException e){
            //lol
        }
        hashedPortAlive.add(hashedKey);
        Collections.sort(hashedPortAlive);
        int idx = hashedPortAlive.indexOf(hashedKey);
        int temp;
        for (int i = 1; i <= 3; i++) {
            temp = (idx + i) % hashedPortAlive.size();
            Log.i(TAG, "isThisMyNode: temp = " + temp + "  " + key);
            if (portMap.get(hashedPortAlive.get(temp)).equals(minePort)) {
                hashedPortAlive.remove(hashedKey);
                return true;
            }
        }
        hashedPortAlive.remove(hashedKey);
        return false;
    }

    public void restartServer() {
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }
    }

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        minePort = portStr;
        meraLambaPort = myPort;
        portAlive.add("5554");
        portAlive.add("5556");
        portAlive.add("5558");
        portAlive.add("5560");
        portAlive.add("5562");

        try {
            mineHashedPort = genHash(minePort);
            hashedPortAlive.add(genHash("5554"));
            hashedPortAlive.add(genHash("5556"));
            hashedPortAlive.add(genHash("5558"));
            hashedPortAlive.add(genHash("5560"));
            hashedPortAlive.add(genHash("5562"));

            portMap.put(genHash("5554"), "5554");
            portMap.put(genHash("5556"), "5556");
            portMap.put(genHash("5558"), "5558");
            portMap.put(genHash("5560"), "5560");
            portMap.put(genHash("5562"), "5562");
        }catch (NoSuchAlgorithmException e) {}
        Collections.sort(portAlive);
        Collections.sort(hashedPortAlive);
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, minePort, "init");
        return false;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        // You do not need to implement this.
        if(keys.contains(selection)){
            File file = new File(selection);
            file.delete();
            keys.remove(selection);
            if (selectionArgs == null && selectionArgs.length == 0) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, selection, "delete");
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        Log.v("insert", values.toString());
        String value = (String) values.get("value");
        String tKey = (String) values.get("key");
        String key;
        if (tKey.contains(":")) {
            String splitMsg[] = tKey.split(":");
            key = splitMsg[0];
        }
        else {
            key = tKey;
        }
        String hashedKey;
        String msgToSend = key + "," + value;
        delete(uri, key, new String[]{"single"});
        try {
            hashedKey = genHash(key);
            keys.add(key);
            FileOutputStream out;
            out = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            out.write(value.getBytes());
            out.close();
            Log.i("shahyash", "insert: Key = " + key + " hashed key = " + hashedKey);

        } catch (Exception e) {
            Log.e("shahyash", "insert: IOException in insert" + hashedPortAlive.size());
        }
        if (!tKey.contains(":")) {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, "multicast");
        }
        return null;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        Log.v("query", selection);
        char count;
        String columnNames[] = {"key", "value"};
        MatrixCursor mCursor = new MatrixCursor(columnNames);
        StringBuilder stringBuilder = new StringBuilder();
        FileInputStream fileInputStream;

        if (selection.equals("@")) {
            for (int x = 0; x < keys.size(); x++) {
                if(isThisMyNode(keys.get(x))) {
                    stringBuilder = new StringBuilder();
                    try {
                        Log.i("shahyash", "query:Key = " + keys.get(x));
                        fileInputStream = getContext().openFileInput(keys.get(x));
                        for (int i; (i = fileInputStream.read()) != -1; ) {
                            count = (char) i;
                            stringBuilder.append(count);
                        }
                    } catch (Exception e) {
                        Log.e("shahyash", "query: IOException Encountered: ", e);
                    }

                    String value = stringBuilder.toString();
                    String row[] = {keys.get(x), value};
                    mCursor.addRow(row);
                }
            }
        }
        else if (selection.equals("*")) {
            for (int x = 0; x < keys.size(); x++) {
                stringBuilder = new StringBuilder();
                try {
                    Log.i("shahyash", "query:Key = " + keys.get(x));
                    fileInputStream = getContext().openFileInput(keys.get(x));
                    for (int i; (i = fileInputStream.read()) != -1; ) {
                        count = (char) i;
                        stringBuilder.append(count);
                    }
                } catch (Exception e) {
                    Log.e("shahyash", "query: IOException Encountered: ", e);
                }
                String value = stringBuilder.toString();
                String row[] = {keys.get(x), value};
                mCursor.addRow(row);
            }
        }
        else if (selection.contains(":")) {
            String[] splitMsg = selection.split(":");
            String port = splitMsg[1];
            for (int x = 0; x < keys.size(); x++) {
                stringBuilder = new StringBuilder();
                try {
                    Log.i("shahyash", "query:Key = " + keys.get(x));
                    fileInputStream = getContext().openFileInput(keys.get(x));
                    for (int i; (i = fileInputStream.read()) != -1; ) {
                        count = (char) i;
                        stringBuilder.append(count);
                    }
                } catch (Exception e) {
                    Log.e("shahyash", "query: IOException Encountered: ", e);
                }
                String value = stringBuilder.toString();
                String row[] = {keys.get(x), value};
                String msgToSend = keys.get(x) + "_" + value + "_" + port;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, "sendmsg");
                mCursor.addRow(row);
            }
        }
        else {
            try {
                fileInputStream = getContext().openFileInput(selection);
                for (int i; (i = fileInputStream.read()) != -1; ) {
                    count = (char) i;
                    stringBuilder.append(count);
                }
                String value = stringBuilder.toString();
                String row[] = {selection, value};
                mCursor.addRow(row);
            } catch (Exception e) {
                Log.e("shahyash", "query: IOException Encountered: " + selection);
            }
        }
        Log.i(TAG, "query: " + DatabaseUtils.dumpCursorToString(mCursor));
        return mCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            ServerSocket serverSocket = sockets[0];
            Socket clientSocket;
            DataInputStream msgIn;
            String msgReceived;
            try {
                clientSocket = serverSocket.accept();
                msgIn = new DataInputStream(clientSocket.getInputStream());
                msgReceived = msgIn.readUTF();
                while (msgReceived != null) {
                    if (msgReceived.contains(":")) {
                        String[] splitMsg = msgReceived.split(":");
                        String selection = "fwdmsgs:" + splitMsg[0];
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority("content://edu.buffalo.cse.cse486586.simpledht.provider");
                        uriBuilder.scheme("content");
                        Uri mUri = uriBuilder.build();
                        query(mUri, null, selection, null, null, null);
                    }
                    if (msgReceived.contains(",")) {
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
                        uriBuilder.scheme("content");
                        Uri mUri = uriBuilder.build();
                        String[] splitMsg = msgReceived.split(",");
                        ContentValues mNewValues = new ContentValues();
                        //Log.i(TAG, "doInBackground: break1");
                        mNewValues.put("key", splitMsg[0] + ":");
                        //Log.i(TAG, "doInBackground: break2");
                        mNewValues.put("value", splitMsg[1]);
                        insert(mUri, mNewValues);
                    }
                    if (msgReceived.contains("~")) {
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
                        uriBuilder.scheme("content");
                        Uri mUri = uriBuilder.build();
                        String[] splitMsg = msgReceived.split("~");
                        //Log.i(TAG, "doInBackground: break1");
                        //Log.i(TAG, "doInBackground: break2");
                        delete(mUri, splitMsg[0], null);
                    }
                    clientSocket = serverSocket.accept();
                    msgIn = new DataInputStream(clientSocket.getInputStream());
                    msgReceived = msgIn.readUTF();
                }
            } catch (IOException e) {
                Log.i(TAG, "doInBackground: Exception in reading message\n" + e);
                restartServer();
            }

            //Log.i(TAG, "Recieved message: " + message);
            return null;
        }

    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            DataOutputStream msgOut;
            try {
                if (msgs[1].equals("init")) {
                    int idx = portAlive.indexOf(minePort);
                    idx = (idx + 1) % 5;
                    String portNo = portAlive.get(idx);
                    String msgToSend = msgs[0] + ":" + "imback";
                    Socket socket0 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            (Integer.parseInt(portNo)*2));
                    msgOut = new DataOutputStream(socket0.getOutputStream());
                    msgOut.writeUTF(msgToSend);
                    msgOut.flush();
                    Log.i(TAG, "doInBackground: recovery msg sent: " + msgToSend + ":" + portNo);

                }
                if (msgs[1].equals("multicast")) {
                    String msgToSend = msgs[0];
                    for (String portNo : portAlive) {
                        if (!portNo.equals(minePort)) {
                            Socket socket0 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    (Integer.parseInt(portNo)*2));
                            msgOut = new DataOutputStream(socket0.getOutputStream());
                            msgOut.writeUTF(msgToSend);
                            msgOut.flush();
                        }
                    }
                }
                if (msgs[1].equals("sendmsg")) {
                    String splitMsg[] = msgs[0].split("_");
                    String portNo = splitMsg[2];
                    String msgToSend = splitMsg[0] + "," + splitMsg[1];
                    Socket socket0 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            (Integer.parseInt(portNo)*2));
                    msgOut = new DataOutputStream(socket0.getOutputStream());
                    msgOut.writeUTF(msgToSend);
                    msgOut.flush();
                }
                if (msgs[1].equals("delete")) {
                    String msgToSend = msgs[0]+"~delete";
                    for (String portNo : portAlive) {
                        if (!portNo.equals(minePort)) {
                            Socket socket0 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    (Integer.parseInt(portNo)*2));
                            msgOut = new DataOutputStream(socket0.getOutputStream());
                            msgOut.writeUTF(msgToSend);
                            msgOut.flush();
                        }
                    }
                }

            }catch(Exception e) {}
            return null;
        }
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
}