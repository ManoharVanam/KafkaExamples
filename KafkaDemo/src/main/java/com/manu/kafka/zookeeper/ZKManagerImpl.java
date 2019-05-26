package com.manu.kafka.zookeeper;

import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.manu.kafka.utils.MyFileWriter;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;

public class ZKManagerImpl {
    private static ZooKeeper zkeeper;
    private static ZKConnection zkConnection;

    public ZKManagerImpl(String ip) throws IOException, InterruptedException {
        initialize(ip);
    }

    private void initialize(String ip) throws IOException, InterruptedException {
        zkConnection = new ZKConnection();
        zkeeper = zkConnection.connect(ip);
    }

    public void closeConnection() throws InterruptedException {
        zkConnection.close();
    }

    public void create(String path, byte[] data) throws KeeperException, InterruptedException {

        zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public Object getZNodeData(String path, boolean watchFlag)
            throws KeeperException, InterruptedException, UnsupportedEncodingException {

        byte[] b = null;
        b = zkeeper.getData(path, null, null);
        return new String(b, "UTF-8");
    }

    public void update(String path, byte[] data) throws KeeperException, InterruptedException {
        int version = zkeeper.exists(path, true).getVersion();
        zkeeper.setData(path, data, version);
    }

    // Method to check existence of znode and its status, if znode is available.
    public static void delete(String path) throws KeeperException, InterruptedException {
        zkeeper.delete(path, zkeeper.exists(path, true).getVersion());
    }

    public static void list(String path) throws KeeperException, InterruptedException, IOException {
        List<String> children = zkeeper.getChildren(path, false);
        for (int i = 0; i < children.size(); i++) {
            System.out.println(children.get(i));
        }
        MyFileWriter.write(children);
    }

    public static void main(String[] args) {
        try {
//			String path = "/testing/café";
//            String path = "/testing";
            if (args.length != 3) {
                System.out.println("usage : ZKManagerImpl zk-ip  zkPath operationtype:[LIST, DELETE]");
            }
            String zkIp = args[0];
            String zkPath = args[1];
            Type type = Type.valueOf(args[2]);
            ZKManagerImpl zkImpl = new ZKManagerImpl(zkIp);
//			zkImpl.create(path, "test1".getBytes());
            switch (type) {

                case DELETE:
                    delete(zkPath);
                    break;
                case LIST:
                    list(zkPath);
                    break;
            }
            System.out.println("Successful..");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class ZKConnection {
        private ZooKeeper zoo;
        CountDownLatch connectionLatch = new CountDownLatch(1);

        public ZooKeeper connect(String host) throws IOException, InterruptedException {
            zoo = new ZooKeeper(host, 2181, new Watcher() {
                public void process(WatchedEvent we) {
                    if (we.getState() == KeeperState.SyncConnected) {
                        connectionLatch.countDown();
                    }
                }
            });

            connectionLatch.await();
            return zoo;
        }

        public void close() throws InterruptedException {
            zoo.close();
        }
    }

    enum Type {
        DELETE, LIST
    }

}

