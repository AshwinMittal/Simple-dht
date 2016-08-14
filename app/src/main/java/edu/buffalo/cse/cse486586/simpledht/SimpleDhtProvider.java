package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.text.LoginFilter;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.widget.TextView;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();

    static final String JOIN_PORT = "11108";
    static final int SERVER_PORT = 10000;
    private HashMap<String, String> messages = new HashMap<String, String>();
    private int queryAllFlag = 0;
    private String succNodeId;
    private String predNodeId;
    private String myNodeId;
    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        MsgObj msgObj = new MsgObj("DELETE",myNodeId,selection,"NULL");
        if(selection.equals("*")){
            String[] filenames = getContext().fileList();
            for (String filename : filenames) {
                getContext().deleteFile(filename);
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "MSG" + msgObj.createMsg(),
                    String.valueOf((Integer.parseInt(succNodeId) * 2)));
            return 0;
        }
        else if(selection.equals("@")){
            String[] filenames = getContext().fileList();
            for (String filename : filenames) {
                getContext().deleteFile(filename);
            }
        }
        else{
            try {
                if(!checkHashVal(selection)){
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "MSG" + msgObj.createMsg(),
                            String.valueOf((Integer.parseInt(succNodeId) * 2)));
                    return 0;
                }
                else {
                    getContext().deleteFile(selection);
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
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
        MsgObj msgObj = new MsgObj("INSERT",myNodeId,values.get("key").toString(),values.get("value").toString());
        try {
            if(!checkHashVal(msgObj.msgKey)){
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "MSG" + msgObj.createMsg(),
                        String.valueOf((Integer.parseInt(succNodeId) * 2)));
                return uri;
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        String filename = values.get("key").toString();
        String msg = values.get("value").toString() + "\n";
        FileOutputStream outputStream;
        try {
            outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
            outputStream.write(msg.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }
        Log.v("insert", values.toString());
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        final String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        myNodeId = portStr;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        succNodeId = myNodeId;
        predNodeId = myNodeId;

        if(!JOIN_PORT.equals(myPort)){
            MsgObj joinMsg = new MsgObj("JOIN", myNodeId, myNodeId, predNodeId);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "MSG" + joinMsg.createMsg(), JOIN_PORT);
        }

        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        String[] columnNames= {"key", "value"};
        MatrixCursor cursor = new MatrixCursor(columnNames);

        if(selection.equals("*")){
            String[] filenames = getContext().fileList();
            for(String filename : filenames){
                String msg = "";
                try{
                    InputStream is = getContext().openFileInput(filename);
                    BufferedInputStream bis = new BufferedInputStream(is);
                    while(bis.available()>0){
                        msg += (char) bis.read();
                    }
                }
                catch (Exception e){

                }
                messages.put(filename,msg);
            }
            MsgObj msgObj = new MsgObj("QRYALL",myNodeId,succNodeId,"NULL");
            sendMsg("MSG"+msgObj.createMsg(),String.valueOf((Integer.parseInt(succNodeId) * 2)));
            while(queryAllFlag == 0){
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //Log.d("HashMapSize", Integer.toString(messages.size()));
            Iterator it = messages.entrySet().iterator();
            while (it.hasNext()) {
                HashMap.Entry pair = (HashMap.Entry)it.next();
                cursor.addRow(new String[]{pair.getKey().toString(), pair.getValue().toString()});
            }
        }
        else if(selection.equals("@")){
            String[] filenames = getContext().fileList();
            for(String filename : filenames){
                String msg = "";

                try{
                    InputStream is = getContext().openFileInput(filename);
                    BufferedInputStream bis = new BufferedInputStream(is);
                    while(bis.available()>0){
                        msg += (char) bis.read();
                    }
                }
                catch (Exception e){

                }
                cursor.addRow(new String[]{filename, msg.trim()});
            }
        }
        else{
            try {
                if(checkHashVal(selection)){
                    String msg = "";
                    try{
                        InputStream is = getContext().openFileInput(selection);
                        BufferedInputStream bis = new BufferedInputStream(is);
                        while(bis.available()>0){
                            msg += (char) bis.read();
                        }
                    }
                    catch (Exception e){

                    }
                    cursor.addRow(new String[]{selection, msg.trim()});
                }
                else{
                    MsgObj msgObj = new MsgObj("QRY",myNodeId,selection,"NULL");
                    sendMsg("MSG"+msgObj.createMsg(),String.valueOf((Integer.parseInt(succNodeId) * 2)));
                    while(messages.get(selection) == null){
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    cursor.addRow(new String[]{selection, messages.get(selection)});
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        Log.v("query", selection);
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
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

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try
            {
                while(true){
                    Socket socket = serverSocket.accept();
                    BufferedReader bis = new BufferedReader(new InputStreamReader(socket.getInputStream(),"UTF-8"));
                    String rcvMsg = bis.readLine().trim();
                    rcvMsg = rcvMsg.substring(rcvMsg.indexOf("G")+1, rcvMsg.length());
                    Log.d("MsgRcvd",rcvMsg);
                    String[] parseMsg = rcvMsg.split("~~");
                    MsgObj msgObj = new MsgObj(parseMsg[0],parseMsg[1],parseMsg[2],parseMsg[3]);

                    if(msgObj.msgType.equals("JOIN")){
                        joinFun(msgObj);
                    }
                    else if(msgObj.msgType.equals("SET_PREDSUCC")){
                        if(!msgObj.msgKey.equals("SET_SUCC")) predNodeId = msgObj.msgKey;
                        succNodeId = msgObj.msgValue;
                    }
                    else if(msgObj.msgType.equals("INSERT")){
                        if(checkHashVal(msgObj.msgKey)){
                            ContentValues cv = new ContentValues();
                            cv.put("key", msgObj.msgKey);
                            cv.put("value", msgObj.msgValue);
                            getContext().getContentResolver().insert(mUri, cv);
                        }
                        else {
                            sendMsg("MSG"+msgObj.createMsg(), String.valueOf((Integer.parseInt(succNodeId) * 2)));
                        }
                    }
                    else if(msgObj.msgType.equals("QRY")){
                        if(checkHashVal(msgObj.msgKey)){
                            String getMsg = "";
                            try{
                                InputStream inpStrm = getContext().openFileInput(msgObj.msgKey);
                                BufferedInputStream buff = new BufferedInputStream(inpStrm);
                                while(buff.available()>0){
                                    getMsg += (char) buff.read();
                                }
                            }
                            catch (Exception e){

                            }
                            msgObj.msgType = "RCV";
                            msgObj.msgValue = getMsg;
                            sendMsg("MSG"+msgObj.createMsg(), String.valueOf((Integer.parseInt(msgObj.msgSender) * 2)));
                        }
                        else{
                            sendMsg("MSG"+msgObj.createMsg(), String.valueOf((Integer.parseInt(succNodeId) * 2)));
                        }
                    }
                    else if(msgObj.msgType.equals("RCV")){
                        messages.put(msgObj.msgKey, msgObj.msgValue);
                    }
                    else if(msgObj.msgType.equals("QRYALL")){
                        if(msgObj.msgSender.equals(myNodeId)){
                            queryAllFlag = 1;
                        }
                        else{
                            if(msgObj.msgValue.equals("NULL")){
                                msgObj.msgKey=""; msgObj.msgValue="";
                            }
                            String[] filenames = getContext().fileList();
                            if(filenames.length>0){
                                for(String filename : filenames){
                                    String getMsg = "";
                                    try{
                                        InputStream inpStrm = getContext().openFileInput(filename);
                                        BufferedInputStream buff = new BufferedInputStream(inpStrm);
                                        while(buff.available()>0){
                                            getMsg += (char) buff.read();
                                        }
                                    }
                                    catch (Exception e){

                                    }
                                    msgObj.msgValue += getMsg+"##";
                                    msgObj.msgKey += filename+"##";
                                }
                                msgObj.msgType = "RCVALL";
                                //Log.d("toSender", msgObj.createMsg());
                                sendMsg("MSG" + msgObj.createMsg(), String.valueOf((Integer.parseInt(msgObj.msgSender) * 2)));
                            }

                            msgObj.msgType = "QRYALL";
                            msgObj.msgKey = succNodeId;
                            msgObj.msgValue = "NULL";
                            //Log.d("toSucc", msgObj.createMsg());
                            sendMsg("MSG" + msgObj.createMsg(), String.valueOf((Integer.parseInt(succNodeId) * 2)));
                        }
                    }
                    else if(msgObj.msgType.equals("RCVALL")){
                        String[] keys = msgObj.msgKey.split("##");
                        String[] Vals = msgObj.msgValue.split("##");
                        for(int i=0; i<Vals.length; i++){
                            messages.put(keys[i],Vals[i]);
                        }
                    }
                    else if(msgObj.msgType.equals("DELETE")){
                        if(msgObj.msgKey.equals("*")){
                            String[] filenames = getContext().fileList();
                            if(filenames.length>0){
                                for (String filename : filenames) {
                                    getContext().deleteFile(filename);
                                }
                            }
                            if(!succNodeId.equals(msgObj.msgSender)){
                                sendMsg("MSG" + msgObj.createMsg(), String.valueOf((Integer.parseInt(succNodeId) * 2)));
                            }
                        }
                        else{
                            if(checkHashVal(msgObj.msgKey)){
                                getContext().deleteFile(msgObj.msgKey);
                            }
                            else{
                                sendMsg("MSG" + msgObj.createMsg(), String.valueOf((Integer.parseInt(succNodeId) * 2)));
                            }
                        }
                    }
                    socket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            return null;
        }

        public void joinFun(MsgObj msgObj) throws NoSuchAlgorithmException {
            if(checkHashVal(msgObj.msgKey)){
                String thisMsgKey = msgObj.msgKey;
                String msgSender = msgObj.msgSender;
                String myPred = predNodeId;

                predNodeId = thisMsgKey;
                msgObj.msgType = "SET_PREDSUCC";
                msgObj.msgSender = myNodeId;
                msgObj.msgKey = myPred;
                msgObj.msgValue = myNodeId;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "MSG"+msgObj.createMsg(),
                        String.valueOf((Integer.parseInt(msgSender) * 2)));
                if(myNodeId.equals(myPred)){
                    succNodeId = thisMsgKey;
                }
                else{
                    MsgObj msgObj1 = new MsgObj("SET_PREDSUCC", myNodeId, "SET_SUCC", thisMsgKey);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "MSG"+msgObj1.createMsg(),
                            String.valueOf((Integer.parseInt(myPred) * 2)));
                }
            }
            else{
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "MSG"+msgObj.createMsg(),
                        String.valueOf((Integer.parseInt(succNodeId) * 2)));
            }
        }
    }

    public boolean checkHashVal(String key) throws NoSuchAlgorithmException {
        String predHash = genHash(predNodeId);
        String myHash = genHash(myNodeId);

        if(predHash.compareTo(myHash)==0) return true;
        String keyHash = genHash(key);
        if(predHash.compareTo(myHash)>0 && (keyHash.compareTo(predHash)>0 || keyHash.compareTo(myHash)<=0)) return true;
        if(keyHash.compareTo(predHash)>0 && keyHash.compareTo(myHash)<=0) return true;
        return false;
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String msgToSend = msgs[0];
            String remotePort = msgs[1];
            sendMsg(msgToSend, remotePort);
            return null;
        }
    }
    private void sendMsg(String msgToSend, String remotePort){
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(remotePort));

            OutputStream outputStream = socket.getOutputStream();
            DataOutputStream dos = new DataOutputStream(outputStream);
            dos.writeUTF(msgToSend);
            socket.close();
        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, "ClientTask socket IOException");
        }
    }
}
