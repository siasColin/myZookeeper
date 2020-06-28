package cn.colin.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * @Package: cn.colin.zookeeper
 * @Author: sxf
 * @Date: 2020-6-27
 * @Description:
 */
public class CuratorNodeCacheTest {
    public static void main(String[] args) {

        try {

            String testPath="Locks";
            //创建连接
            CuratorFramework client= CuratorFrameworkFactory.builder()
                    //设置zookeeper连接串
                    .connectString("192.168.0.133:2181")
                    //设置重连机制
                    .retryPolicy(new ExponentialBackoffRetry(100, 6))
                    //设置会话超时时间
                    .sessionTimeoutMs(5000)
                    //设置连接超时时间
                    .connectionTimeoutMs(6000)
                    .build();
            client.start();
            //如果testPath存在，删除路径
            Stat stat = client.checkExists().forPath("/"+testPath);
            if(stat != null)
            {
                client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/"+testPath);
            }
            //创建testPath
            client.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/"+testPath,testPath.getBytes());
            //创建NodeCache，监控节点内容变化。
            final NodeCache nodeCache = new NodeCache(client,"/" + testPath);
            nodeCache.start(true);
            //创建listenner
            nodeCache.getListenable().addListener(new NodeCacheListener() {
                @Override
                public void nodeChanged() throws Exception {
                    System.out.println("节点内容变化：" + new String(nodeCache.getCurrentData().getData()));
                }
            });
            //第一次修改节点
            client.setData().forPath("/" + testPath,"1".getBytes());
            //此处如果不睡眠，两次修改时间太近，第一次修改的内容会丢失，监听器会察觉不到第一次修改。
            Thread.sleep(1000);
            //第二次修改节点
            client.setData().forPath("/" + testPath,"2".getBytes());
            Thread.sleep(1000);
            //关闭
            nodeCache.close();
        } catch (Exception e) {
            e.printStackTrace();
            // TODO: handle exception
        }

    }
}
