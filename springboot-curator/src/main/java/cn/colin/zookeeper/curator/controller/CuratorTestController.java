package cn.colin.zookeeper.curator.controller;

import cn.colin.zookeeper.curator.helper.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.List;

/**
 * @Package: cn.colin.zookeeper.curator.controller
 * @Author: sxf
 * @Date: 2020-6-27
 * @Description:
 */
@RestController
@RequestMapping("/curator")
public class CuratorTestController {
    @Autowired
    private ZkClient zkClient;

    /**
     * 创建节点
     * @return
     * @throws Exception
     */
    @RequestMapping("/create")
    public String create() throws Exception {
        CuratorFramework client = zkClient.getClient();
        String znode = client.create()
                //递归创建节点，当要创建的节点父节点不存在时可以自动创建
                .creatingParentsIfNeeded()
                /**
                 * 节设置点类型
                 *      CreateMode.PERSISTENT：持久化节点
                 *      CreateMode.PERSISTENT_SEQUENTIAL：持久化有序节点
                 *      CreateMode.EPHEMERAL：临时节点
                 *      CreateMode.EPHEMERAL_SEQUENTIAL：临时有序节点
                 */
                .withMode(CreateMode.PERSISTENT)
                /**
                 * 设置节点的权限
                 *      OPEN_ACL_UNSAFE：完全开放 相当于 world:anyone:cdrwa
                 *      CREATOR_ALL_ACL：L赋予那些授权了的用户具备权限
                 *      READ_ACL_UNSAFE：赋予用户读的权限，也就是获取数据之类的权限
                 * 自定义权限列表：
                 *          List<ACL> list = new ArrayList<ACL>();
                 *          //授权模式和授权对象，这里指定ip授权，授权给192.168.0.135
                 *          Id id = new Id("ip","192.168.0.135");
                 *          list.add(new ACL(ZooDefs.Perms.ALL,id));
                 */
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                /*//异步创建节点
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                        System.out.println("节点路径："+curatorEvent.getPath());
                    }
                })*/
                //arg1:节点路径，arg2节点的数据
                .forPath("/curatorNode1","curatorNode1".getBytes());
        return znode;
    }

    /**
     * 读取节点数据
     * @return
     * @throws Exception
     */
    @RequestMapping("/read")
    public String read() throws Exception {
        CuratorFramework client = zkClient.getClient();
        //读取节点数据时，同时获取节点属性
        Stat stat = new Stat();
        byte [] bytes = client.getData()
                //读取节点属性
                .storingStatIn(stat)
                .forPath("/curatorNode1");
        return new String(bytes);
    }

    /**
     * 读取子节点
     * @return
     * @throws Exception
     */
    @RequestMapping("/getChildren")
    public Object getChildren() throws Exception {
        CuratorFramework client = zkClient.getClient();
        List<String> childrenList =  client.getChildren()
                .forPath("/");
        return childrenList;
    }

    /**
     * 检查节点是否存在
     * @return
     * @throws Exception
     */
    @RequestMapping("/exists")
    public Object exists() throws Exception {
        CuratorFramework client = zkClient.getClient();
        //如果不存在，返回null
        Stat stat = client.checkExists()
                .forPath("/curatorNode1");
        return stat;
    }


    /**
     * 更新节点：
     *      写入节点数据
     * @return
     * @throws Exception
     */
    @RequestMapping("/write")
    public Object write() throws Exception {
        CuratorFramework client = zkClient.getClient();
        Stat stat = client.setData()
                //带版本号更新，-1 表示版本号不参与更新控制
//                .withVersion(-1)
                /*//异步更新
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                        System.out.println("节点路径："+curatorEvent.getPath());
                    }
                })*/
                .forPath("/curatorNode1","curatorNode11".getBytes());
        return stat;
    }

    /**
     * 删除节点
     * @return
     * @throws Exception
     */
    @RequestMapping("/delete")
    public String delete() throws Exception {
        CuratorFramework client = zkClient.getClient();
        client.delete()
                //递归删除子节点，如果不加则删除带有子节点的节点时会抛异常
//                .deletingChildrenIfNeeded()
                //带版本号删除，-1 表示版本号不参与删除控制
                .withVersion(-1)
                /*//异步删除
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                        System.out.println("节点路径："+curatorEvent.getPath());
                    }
                })*/
                .forPath("/curatorNode1");
        return "delete Success";
    }


    /**
     * NodeCache
     *      监听指定的节点，监听节点的增、删、改
     * @return
     * @throws Exception
     */
    @RequestMapping("/watcherNode")
    public String watcherNode() throws Exception {
        CuratorFramework client = zkClient.getClient();
        final NodeCache nodeCache = new NodeCache(client,"/curatorNode1");
        //启动监视器对象,buildInitial: 初始化的时候获取node的值并且缓存
        nodeCache.start(true);
        if(nodeCache.getCurrentData() != null){
            System.out.println("节点的初始化数据为："+new String(nodeCache.getCurrentData().getData()));
        }else{
            System.out.println("节点初始化数据为空。。。");
        }
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                if(nodeCache.getCurrentData() == null){
                    System.out.println("节点已删除");
                }else{
                    //获取当前数据
                    String data = new String(nodeCache.getCurrentData().getData());
                    System.out.println("节点路径为："+nodeCache.getCurrentData().getPath()+" 数据: "+data);
                }

            }
        });
        return "watcherNode Success";
    }

    /**
     * PathChildrenCache
     *      监听子节点变化，增、删、改
     * @return
     * @throws Exception
     */
    @RequestMapping("/watcherChildren")
    public String watcherChildren() throws Exception {
        CuratorFramework client = zkClient.getClient();
        /**
         * arg1:连接对象
         * arg2:监视的根节点路径
         * arg3:事件中是否可以获取节点的数据
         */
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client,"/",true);
        //启动监听
        pathChildrenCache.start();
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                //事件类型，CHILD_ADDED、CHILD_UPDATED、CHILD_REMOVED
                System.out.println("事件类型："+pathChildrenCacheEvent.getType());
                //节点路径
                System.out.println("节点路径："+pathChildrenCacheEvent.getData().getPath());
                //节点数据
                System.out.println("节点数据："+new String(pathChildrenCacheEvent.getData().getData()));
            }
        });
        return "watcherChildren Success";
    }
    /**
     * 事务
     * @return
     * @throws Exception
     */
    @RequestMapping("/transaction")
    public Object transaction() throws Exception {
        CuratorFramework client = zkClient.getClient();
        //开启事务
        Collection<CuratorTransactionResult> collection = client.inTransaction()
                .create().forPath("/transactionNode","transactionNode".getBytes())
                .and()
                //此时transactionNode1 是不存在的会抛异常，由于加入了事务transactionNode节点也不会被创建
                .setData().forPath("/transactionNode1","transactionNode1".getBytes())
                .and()
                //提交事务
                .commit();
        return collection;
    }

    /**
     * 分布式可重入排它锁
     * @return
     * @throws Exception
     */
    @RequestMapping("/interProcessLock")
    public String interProcessLock() throws Exception {
        CuratorFramework client = zkClient.getClient();
        //创建锁
        InterProcessLock interProcessLock = new InterProcessMutex(client,"/interProcessLock");
        try {
            System.out.println("等待获取锁！！！！");
            //获取锁
            interProcessLock.acquire();
            System.out.println("成功获取锁！！！！");
            //执行业务逻辑
            Thread.sleep(10000);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            //释放锁
            interProcessLock.release();
            System.out.println("锁已释放！！！！");
        }
        return "interProcessLock";
    }

    /**
     * 读写锁
     * @return
     * @throws Exception
     */
    @RequestMapping("/interProcessReadWriteLock")
    public String interProcessReadWriteLock() throws Exception {
        CuratorFramework client = zkClient.getClient();
        //读写锁
        InterProcessReadWriteLock interProcessReadWriteLock = new InterProcessReadWriteLock(client,"/interProcessReadWriteLock");
        //创建读锁，多个读锁可以并行执行
        InterProcessLock interProcessLock = interProcessReadWriteLock.readLock();
        //创建写锁
//        InterProcessLock interProcessLock = interProcessReadWriteLock.writeLock();
        try {
            System.out.println("等待获取锁！！！！");
            //获取锁
            interProcessLock.acquire();
            System.out.println("成功获取锁！！！！");
            //执行业务逻辑
            Thread.sleep(10000);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            //释放锁
            interProcessLock.release();
            System.out.println("锁已释放！！！！");
        }
        return "interProcessReadWriteLock";
    }

}
