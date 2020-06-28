package cn.colin.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @Package: cn.colin.zookeeper
 * @Author: sxf
 * @Date: 2020-6-26
 * @Description: 分布式锁
 *  分布式锁有多种实现方式，比如通过数据库、redis都可实现。作为分布式协同工具ZooKeeper，当然也有着标准的实现方式。
 * 下面介绍在zookeeper中如何实现排他锁。
 * 设计思路：
 *      1.每个客户端往/Locks下创建临时有序节点/Locks/Lock，创建成功后/Locks下面会有每个客户端对应的节点，如/Locks/Lock_000000001
 *      2.客户端取得/Locks下子节点，并进行排序，判断排在最前面的是否为自己，如果自己的锁节点在第一位，代表获取锁成功
 *      3.如果自己的锁节点不在第一位，则监听自己前一位的锁节点。例如，自己锁节点Lock_000000002，那么则监听Lock_000000001
 *      4.当前一位锁节点（Lock_000000001）对应的客户端执行完成，释放了锁，将会触发监听客户端(Lock_000000002)的逻辑
 */
public class MyLock {
    //  zk的连接串
    private String connectString = "192.168.0.133:2181";
    //计数器对象
    CountDownLatch countDownLatch = new CountDownLatch(1);
    //ZooKeeper配置信息
    ZooKeeper zooKeeper;
    private static final String LOCK_ROOT_PATH = "/Locks";
    private static final String LOCK_NODE_NAME = "Lock_";
    //锁节点路径
    private String lockPath;

    public MyLock(){
        //通过构造方法初始化zooKeeper
        try{
            zooKeeper = new ZooKeeper(connectString, 5000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if(event.getType() == Event.EventType.None){
                        //连接成功
                        if(event.getState() == Event.KeeperState.SyncConnected){
                            countDownLatch.countDown();
                        }
                    }
                }
            });
            countDownLatch.await();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 获取锁
     * @throws Exception
     */
    public void acquireLock() throws Exception{
        //创建锁节点
        createLock();
        //尝试获取锁
        attemptLock();
    }

    /**
     * 创建锁节点
     * @throws Exception
     */
    private void createLock() throws Exception{
        //判断Locks是否存在，不存在则创建
        Stat stat = zooKeeper.exists(LOCK_ROOT_PATH,false);
        if(stat == null){
            zooKeeper.create(LOCK_ROOT_PATH,new byte[0],ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        //创建临时有序节点
        lockPath = zooKeeper.create(LOCK_ROOT_PATH+"/"+LOCK_NODE_NAME,new byte[0],ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    //监视器对象，监视前一个节点是否被删除（即是否已释放了锁）
    Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeDeleted){
                synchronized (this){
                    //通知线程继续执行
                    notifyAll();
                }
            }
        }
    };

    /**
     * 尝试获取锁
     * @throws Exception
     */
    private void attemptLock() throws Exception{
        //获取Locks节点下的所有节点
        List<String> childrenList = zooKeeper.getChildren(LOCK_ROOT_PATH,false);
        //对节点进行排序
        Collections.sort(childrenList);
        //判断当前锁节点在所有节点中的位置
        int index = childrenList.indexOf(lockPath.substring(LOCK_ROOT_PATH.length()+1));
        if(index == 0){
            //获取锁成功
            return;
        }else{
            //监听前一个节点
            String prePath = childrenList.get(index-1);
            Stat stat = zooKeeper.exists(LOCK_ROOT_PATH + "/" + prePath, watcher);
            if(stat == null){//说明上一个节点已经释放了锁
                attemptLock();
            }else{
                synchronized (watcher){
                    //使线程阻塞，等待重新获取锁
                    watcher.wait();
                }
                attemptLock();
            }
        }
    }
    //释放锁
    public void releaseLock() throws Exception{
        //删除临时有序节点
        zooKeeper.delete(this.lockPath,-1);
        zooKeeper.close();
    }
}
