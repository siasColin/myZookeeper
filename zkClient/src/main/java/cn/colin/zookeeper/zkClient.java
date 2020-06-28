package cn.colin.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * @Package: cn.colin.zookeeper
 * @Author: sxf
 * @Date: 2020-6-25
 * @Description:
 */
public class zkClient implements Watcher {
    //存放集群地址 做负载均衡用
    public static List<String> serverList = new ArrayList<String>();
    //请求总数
    private static  int  reqCount = 1;
    //服务个数
    private static int  serverCount = 0; //初始值是0
    //创建一个计数器对象
    CountDownLatch countDownLatch = new CountDownLatch(1);
    private String connectString = "192.168.0.133:2181,192.168.0.133:2182,192.168.0.133:2183";
    //服务信息根节点 服务提供方和服务消费方一致
    private String pNode = "/testServer";
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws Exception {
        //开始服务监听
        zkClient userClient = new zkClient();
        userClient.run();
        //当访问可用时与服务交互
        Scanner scanner = new Scanner(System.in);
        while (true){
            System.out.println("输入要发送的信息(e:退出)");
            String text = scanner.next();
            if (text.equals("e"))System.exit(-1);
            if (serverList.size() == 0){
                System.err.println("没有可用的服务...");
            }else {
                userClient.sendToServer(text);
            }
        }
    }

    private void run() throws Exception {
        /**
         * 连接zookeeper
         *     由于ZooKeeper连接对象创建是异步的，所以对象创建完之后并不代表连接成功，如果下面直接使用zooKeeper对象，可能会报错
         *     这里采用让主线程阻塞等待连接完成的方式处理该问题
         */
        zooKeeper = new ZooKeeper(connectString, 5000, this);
        //主线程阻塞等待连接对象的创建成功
        countDownLatch.await();
        //尝试获取服务信息
        getServerInfo();
    }

    //获取服务信息
    private void getServerInfo()  {
        serverList.clear();
        try {
            //获取根节点下的子节点
            List<String> childrenList = zooKeeper.getChildren(pNode,true);
            for(String cNode : childrenList) {
                //父+子 =完整的路径
                String path = pNode + "/" + cNode;
                byte[] data = zooKeeper.getData(path, true, null);
                String dataStr = new String(data);
                serverList.add(dataStr);
                serverCount=serverList.size();
                System.out.println("获取服务信息成功#############"+dataStr);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //当节点状态发送变化时将执行该方法(通知处理)
    @Override
    public void process(WatchedEvent event) {
        try{
            if(event.getType() == Event.EventType.None){
                if (event.getState() == Event.KeeperState.SyncConnected){
                    System.out.println("连接创建成功！！！");
                    //结束主线程的阻塞，继续往下执行
                    countDownLatch.countDown();
                }else if(event.getState() == Event.KeeperState.Disconnected){
                    System.out.println("连接断开！！！");
                }else if(event.getState() == Event.KeeperState.Expired){
                    System.out.println("会话超时！！！");
                    //重新连接
                    zooKeeper = new ZooKeeper(connectString, 5000, this);
                }else if(event.getState() == Event.KeeperState.AuthFailed){
                    System.out.println("认证失败！！！");
                }
            }else{
                if (event.getPath().startsWith(pNode)) {
                    //根据具体逻辑处理不同的事件类型,此处只关心节点的创建删除和更新
                    if (event.getType() == Event.EventType.NodeCreated) {
                        System.err.println("服务上线了");
                        getServerInfo();
                    } else if (event.getType() == Event.EventType.NodeDataChanged) {
                        System.err.println("服务更新了");
                        getServerInfo();
                    }else if (event.getType()== Event.EventType.NodeDeleted){
                        getServerInfo();
                        System.err.println("服务下线了");
                    }else if (event.getType() == Event.EventType.NodeChildrenChanged){
                        getServerInfo();
                        System.err.println("子节点改变了");
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void sendToServer(String text) {
        //本地负载均衡轮询算法
        String server = serverList.get(reqCount%serverCount );
        String [] serverInfo = server.split(":");
        InetSocketAddress server_address = new InetSocketAddress(serverInfo[0], Integer.parseInt(serverInfo[1]));
        Socket socket = new Socket();
        try {
            socket.connect(server_address);
            //System.out.println("连接服务器成功!");
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(text.getBytes());
            System.out.println("消息发送成功!");
            reqCount++;
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
