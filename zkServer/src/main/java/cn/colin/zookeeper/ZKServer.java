package cn.colin.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;

/**
 * @Package: cn.colin.zookeeper
 * @Author: sxf
 * @Date: 2020-6-25
 * @Description: 服务端
 */
public class ZKServer {
    private String connectString = "192.168.0.133:2181,192.168.0.133:2182,192.168.0.133:2183";
    private String pNode = "/testServer";
    private ZooKeeper zooKeeper;
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        new ZKServer().serving();
    }

    public void serving() throws IOException, KeeperException, InterruptedException {
        //创建一个计数器对象
        CountDownLatch countDownLatch = new CountDownLatch(1);
        /**
         * 由于ZooKeeper连接对象创建是异步的，所以对象创建完之后并不代表连接成功，如果下面直接使用zooKeeper对象，可能会报错
         * 这里采用让主线程阻塞等待连接完成的方式处理该问题
         */
        zooKeeper = new ZooKeeper(connectString, 5000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if(event.getState() == Event.KeeperState.SyncConnected){
                    System.out.println("连接创建成功！！！");
                    //结束主线程的阻塞，继续往下执行
                    countDownLatch.countDown();
                }
            }
        });
        //主线程阻塞等待连接对象的创建成功
        countDownLatch.await();
        Stat stat = zooKeeper.exists(pNode, false);
        if (stat == null){
            //先创建父节点
            zooKeeper.create(pNode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        /**
         * 模拟三个服务，端口号分别为7777,8888,9999
         */
        int [] ports = {7777,8888,9999};
        for (int port : ports) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try{
                        //获取本机ip地址
                        String ip = null;
                        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
                        while (networkInterfaces.hasMoreElements()) {
                            NetworkInterface ni = (NetworkInterface) networkInterfaces.nextElement();
                            Enumeration<InetAddress> nias = ni.getInetAddresses();
                            while (nias.hasMoreElements()) {
                                InetAddress ia = (InetAddress) nias.nextElement();
                                if (!ia.isLinkLocalAddress() && !ia.isLoopbackAddress() && ia instanceof Inet4Address) {
                                    ip = ia.getHostAddress();
                                }
                            }
                        }

                        //启动服务
                        ServerSocket socket = new ServerSocket(port);
                        System.out.println("服务器已启动...");
                        //注册服务
                        serverRegister(ip, port);
                        //处理请求
                        clientHandler(socket);
                    }catch (Exception e){
                        e.printStackTrace();
                    }

                }
            });
            t.start();
        }
    }

    private void clientHandler(ServerSocket socket) throws IOException {
        while (true) {
            Socket accept = socket.accept();
            InputStream inputStream = accept.getInputStream();
            byte[] barr = new byte[1024];
            while (true) {
                int size = inputStream.read(barr);
                if (size == -1) {
                    //System.out.println("客户端已关闭..");
                    accept.close();
                    break;
                }
                String s = new String(barr, 0, size);
                //输出客户端消息
                System.out.println(socket.getLocalPort()+"收到消息："+accept.getInetAddress().getHostAddress() + ": " + s);
                if(s.equals("delete")){
                    accept.close();
                    try {
                        //删除节点，模拟服务掉线，-1：标识version不参与删除条件
                        zooKeeper.delete(pNode+"/"+accept.getInetAddress().getHostAddress()+":"+socket.getLocalPort(),-1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    }
                    break;
                }
            }
        }

    }

    private void serverRegister(String ip, int port) throws IOException, KeeperException, InterruptedException {
        //注册服务
        try {
            ArrayList<ACL> acl = new ArrayList<>();
            acl.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE));
            //CreateMode.EPHEMERAL 临时节点，当拥有该结点的session终止（expire）了，该结点也就销毁了
            zooKeeper.create(pNode+"/"+ip+":"+port, (ip + ":" + port).getBytes(StandardCharsets.UTF_8), acl, CreateMode.EPHEMERAL);
            System.out.println("服务发布成功!");
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
