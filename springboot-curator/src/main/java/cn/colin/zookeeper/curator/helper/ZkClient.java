package cn.colin.zookeeper.curator.helper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @Package: cn.colin.zookeeper.curator.helper
 * @Author: sxf
 * @Date: 2020-6-27
 * @Description: curator 连接 zookeeper公用类
 *  RetryPolicy session重连策略说明：
 *      //3秒后重连一次，只重试一次
 *      RetryPolicy retryPolicy = new RetryOneTime(3000);
 *      //每3秒重连一次，重试3次
 *      RetryPolicy retryPolicy = new RetryNTimes(3,3000);
 *      //每3秒重连一次，总等待时间超过10秒后停止重连
 *      RetryPolicy retryPolicy = new RetryUntilElapsed(10000,3000);
 *      //重连3次，每次重连间隔：baseSleepTimeMs * Math.max(1, random.nextInt(1 << retryCount+1))
 *      RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
 */
public class ZkClient {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private CuratorFramework client;
    //zookeeper连接串
    private String zookeeperServer;
    //命名空间
    private String namespace;
    //会话超时时间
    private int sessionTimeoutMs;
    //连接超时时间
    private int connectionTimeoutMs;
    //重试间隔
    private int baseSleepTimeMs;
    //最大重试次数
    private int maxRetries;

    public void init() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                //设置zookeeper连接串
                .connectString(zookeeperServer)
                //设置重连机制
                .retryPolicy(retryPolicy)
                //设置会话超时时间
                .sessionTimeoutMs(sessionTimeoutMs)
                //设置连接超时时间
                .connectionTimeoutMs(connectionTimeoutMs);
        if(namespace != null && !namespace.trim().equals("")){
            /**
             * 设置命名空间
             *      如果设置了命名空间，那么通过该客户端创建的节点都将有一个统一的根节点 namespace
             */
            builder.namespace(namespace);
        }
        client = builder.build();
        //打开连接
        client.start();
    }

    public void stop() {
        client.close();
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void register() {
        try {
            String rootPath = "/" + "services";
            String hostAddress = InetAddress.getLocalHost().getHostAddress();
            String serviceInstance = "prometheus" + "-" +  hostAddress + "-";
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(rootPath + "/" + serviceInstance);
        } catch (Exception e) {
            logger.error("注册出错", e);
        }
    }

    public List<String> getChildren(String path) {
        List<String> childrenList = new ArrayList<>();
        try {
            childrenList = client.getChildren().forPath(path);
        } catch (Exception e) {
            logger.error("获取子节点出错", e);
        }
        return childrenList;
    }

    public int getChildrenCount(String path) {
        return getChildren(path).size();
    }

    public List<String> getInstances() {
        return getChildren("/services");
    }

    public int getInstancesCount() {
        return getInstances().size();
    }

    public void setZookeeperServer(String zookeeperServer) {
        this.zookeeperServer = zookeeperServer;
    }
    public String getZookeeperServer() {
        return zookeeperServer;
    }
    public void setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }
    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }
    public void setConnectionTimeoutMs(int connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
    }
    public int getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }
    public void setBaseSleepTimeMs(int baseSleepTimeMs) {
        this.baseSleepTimeMs = baseSleepTimeMs;
    }
    public int getBaseSleepTimeMs() {
        return baseSleepTimeMs;
    }
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
    public int getMaxRetries() {
        return maxRetries;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
