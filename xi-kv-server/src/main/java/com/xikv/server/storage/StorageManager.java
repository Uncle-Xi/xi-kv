package com.xikv.server.storage;

import com.xikv.common.thread.XiKVThread;
import com.xikv.common.util.StringUtils;
import com.xikv.server.Request;
import com.xikv.server.config.Configuration;
import com.xikv.server.memory.XiKVDatabase;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * @description: StorageManager
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class StorageManager extends XiKVThread {

    Configuration config;
    XiKVDatabase xiDB;
    private static final String SNAPSHOT_FILE = "snapshot.txt";
    private static final String SNAPSHOT_TIMEOUT_KEY_FILE = "snapshot_timeout_key.txt";
    private static final String LOG_FILE_SUFFIX = "_log.log";
    private static final SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd");

    public StorageManager(Configuration config) {
        super("StorageManager");
        this.config = config;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(5 * 60 * 1000);
                System.out.println("内存快照，开始！");
                snapshot();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void loadData() {
        String dir = config.getStorage();
        loadMap(dir);
        loadKey(dir);
        super.start();
    }

    private void loadMap(String dir) {
        File snapshot = getFile(dir, SNAPSHOT_FILE);
        try {
            InputStreamReader isr = new InputStreamReader(new FileInputStream(snapshot));
            BufferedReader br = new BufferedReader(isr);
            String result;
            int total = 0;
            while ((result = br.readLine()) != null) {
                xiDB.options((Request) StringUtils.getObjectByClazz(result, Request.class));
                total++;
            }
            System.out.println("加载数据条数：" + total);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void loadKey(String dir) {
        File snapshotKey = getFile(dir, SNAPSHOT_TIMEOUT_KEY_FILE);
        try {
            InputStreamReader isr = new InputStreamReader(new FileInputStream(snapshotKey));
            BufferedReader br = new BufferedReader(isr);
            String result;
            int total = 0;
            while ((result = br.readLine()) != null) {
                xiDB.insertAndSortKey((XiKVDatabase.KEY) StringUtils.getObjectByClazz(result, XiKVDatabase.KEY.class));
                total++;
            }
            System.out.println("加载key条数：" + total);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        XiKVDatabase.KEY key = new XiKVDatabase.KEY("ok", 0, XiKVDatabase.Type.STRING);
        String result = StringUtils.getString(key);
        XiKVDatabase.KEY k = (XiKVDatabase.KEY) StringUtils.getObjectByClazz(result, XiKVDatabase.KEY.class);
        System.out.println(k.key);
    }

    private File getFile(String baseDir, String fileName){
        File newFile = new File(baseDir, fileName);
        try {
            if (!newFile.exists()) {
                newFile.createNewFile();
            }
            //System.out.println(newFile.getAbsolutePath());
            //System.out.println(newFile.exists());
        } catch (Exception e){
            e.printStackTrace();
        }
        return newFile;
    }

    public void snapshot() {
        String dir = config.getStorage();
        if (xiDB.getDataSize() < 1){
            System.out.println("没有数据，直接结束...");
            return;
        }
        File snapshot = getFile(dir, SNAPSHOT_FILE);
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(snapshot));
            for (Map.Entry<XiKVDatabase.KEY, Object> me : xiDB.getDATA().entrySet()) {
                Request request = new Request();
                request.setKey(me.getKey().key);
                request.setOpCode(me.getKey().opCode);
                request.setTimeout(me.getKey().timeout);
                request.setType(me.getKey().type);
                request.setValue((String) me.getValue());
                out.write(StringUtils.getString(request) + "\n");
            }
            out.close();
            System.out.println("写入快照，数目：" + xiDB.getDataSize());
        } catch (Exception e){
            e.printStackTrace();
        }
        File snapshotKey = getFile(dir, SNAPSHOT_TIMEOUT_KEY_FILE);
        if (xiDB.getKEY_TIMEOUT().length < 1) {
            System.out.println("没有超时键，直接结束...");
            return;
        }
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(snapshotKey));
            for (XiKVDatabase.KEY key : xiDB.getKEY_TIMEOUT()) {
                out.write(StringUtils.getString(key) + "\n");
            }
            out.close();
            System.out.println("写入超时键，数目：" + xiDB.getKEY_TIMEOUT().length);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    // [op][type][data]
    // get set del incr decr setnx exists
    // string hash set list
    public void log(Request request) {
        String dir = config.getStorage();
        String fileName = sdf.format(new Date()) + LOG_FILE_SUFFIX;
        File snapshot = getFile(dir, fileName);
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(snapshot, true));
            out.write(StringUtils.getString(request) + "\n");
            out.close();
            //System.out.println("写入操作日志完成：" + request);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void setXiDB(XiKVDatabase xiDB) {
        this.xiDB = xiDB;
    }
}
