package com.xikv.server.config;

import com.xikv.common.util.TransferFile;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * @description: Configuration
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class Configuration {

    private int port;
    private String storage;
    private String requirePass;
    private String startModel;
    private int slot;
    private int clusterId;
    private int serverId;
    private Map<Integer, Server> servers = new HashMap<>();
    private Map<Integer, SlotPart> slotParts = new HashMap<>();

    private String masterAddr;
    private List<String> followerAddr;

    public void parse(String path) throws Exception {
        File configFile = TransferFile.getTransferFile(new File(path), path);
        System.out.println("Reading configuration from: " + configFile.getAbsolutePath());
        try {
            if (!configFile.exists()) {
                throw new IllegalArgumentException(configFile.toString() + " file is missing");
            }
            Properties cfg = new Properties();
            FileInputStream in = new FileInputStream(configFile);
            try {
                cfg.load(in);
            } finally {
                in.close();
            }
            parseProperties(cfg);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            TransferFile.deleteFile(configFile);
        }
    }

    public void parseProperties(Properties cpProp) throws Exception {
        for (Map.Entry<Object, Object> entry : cpProp.entrySet()) {
            String key = entry.getKey().toString().trim();
            String value = entry.getValue().toString().trim();
            if (key.equals("storage")) {
                storage = value;
            } else if (key.equals("port")) {
                port = Integer.parseInt(value);
            } else if (key.equals("require.pass")) {
                requirePass = value.trim();
            } else if (key.equals("server.id")) {
                serverId = Integer.parseInt(value);
            } else if (key.equals("slot")) {
                slot = Integer.parseInt(value);
            } else if (key.equals("cluster.id")) {
                clusterId = Integer.parseInt(value);
            } else if (key.startsWith("slot.part.")) {
                splitSlotPart(key.trim(), value.trim());
            } else if (key.startsWith("start.model")) {
                startModel = value.trim();
            } else if (key.startsWith("server.id.")) {
                splitServers(key.trim(), value.trim());
            }
        }
    }

    // slot.part.0=0:16384
    private void splitSlotPart(String key, String val) throws Exception {
        String[] spa = val.split(":");
        int ci = Integer.valueOf(key.replaceAll("slot\\.part\\.", ""));
        SlotPart sp = new SlotPart(ci, Integer.valueOf(spa[0]), Integer.valueOf(spa[1]));
        slotParts.put(ci, sp);
    }

    // server.id.0=127.0.0.1:5268
    private void splitServers(String key, String val) throws Exception {
        String[] spa = val.split(":");
        int sev = Integer.valueOf(key.replaceAll("server\\.id\\.", ""));
        Server sp = new Server(sev, spa[0], Integer.valueOf(spa[1]));
        servers.put(sev, sp);
    }

    public static void main(String[] args) {
        System.out.println("slot.part.0=0:16384".replaceAll("slot\\.part\\.", ""));
    }

    public static class Server {
        private int serverId;
        private String ip;
        private int port;
        private InetSocketAddress electionAddr;

        public Server(int serverId, String ip, int port) {
            this.serverId = serverId;
            this.ip = ip;
            this.port = port;
            this.electionAddr = new InetSocketAddress(ip, port);
        }

        public int getServerId() {
            return serverId;
        }

        public String getIp() {
            return ip;
        }

        public int getPort() {
            return port;
        }

        public InetSocketAddress getElectionAddr() {
            return electionAddr;
        }
    }

    public static class SlotPart {
        private int clusterId;
        private int slotStart;
        private int slotEnd;

        public SlotPart(int clusterId, int slotStart, int slotEnd) {
            this.clusterId = clusterId;
            this.slotStart = slotStart;
            this.slotEnd = slotEnd;
        }

        public int getClusterId() {
            return clusterId;
        }

        public int getSlotStart() {
            return slotStart;
        }

        public int getSlotEnd() {
            return slotEnd;
        }
    }


    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }

    public void setFollowerAddr(List<String> followerAddr) {
        this.followerAddr = followerAddr;
    }

    public int getPort() {
        return port;
    }

    public String getStorage() {
        return storage;
    }

    public String getRequirePass() {
        return requirePass;
    }

    public int getSlot() {
        return slot;
    }

    public int getClusterId() {
        return clusterId;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public List<String> getFollowerAddr() {
        return followerAddr;
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public Map<Integer, Server> getServers() {
        return servers;
    }

    public void setServers(Map<Integer, Server> servers) {
        this.servers = servers;
    }

    public String getStartModel() {
        return startModel;
    }

    public Map<Integer, SlotPart> getSlotParts() {
        return slotParts;
    }
}

