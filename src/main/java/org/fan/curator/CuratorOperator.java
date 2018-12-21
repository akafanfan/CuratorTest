package org.fan.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.RetryNTimes;

import java.util.List;

public class CuratorOperator {
    public CuratorFramework client = null;
    public static final String zkServerPath = "222.24.28.185:2181";

    //实例化客户端
    public CuratorOperator() {
        /**
         * 同步创建zk示例，原生java api是异步的
         *
         * Curator链接zookeeper的策略：ExponentialBackoffRetry
         *     baseSleepTimeMs:初始sleep的时间
         *     maxRtries:最大重试次数
         *     maxSleepMs:最大重试时间
         */
//         RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);

        /**
         * Curator链接zookeeper的策略：RetryNTimes
         *     n:重试次数
         *     sleepMsBetweenRetries:每次重试间隔时间
         */
        RetryPolicy retryPolicy = new RetryNTimes(3, 5000);

        /**
         * Curator链接zookeeper的策略：RetryForever(永远重试)
         *     不推荐
         */
//         RetryPolicy retryPolicy = new RetryForever(retryIntervalMs);

        /**
         * Curator链接zookeeper的策略：RetryUntilElapsed
         *     maxElasedTimeMs:最大重试时间
         *     sleepMsBetweenRetries:每次重试间隔
         *     重试时间超过maxElapsedTimeMs后就不再重试
         */
//         RetryPolicy retryPolicy = new RetryUntilElapsed(2000, 3000);
        client = CuratorFrameworkFactory.builder()
                .connectString(zkServerPath)
                .sessionTimeoutMs(10000)
                .retryPolicy(retryPolicy)
                .namespace("workspace")
                .build();
        client.start();


    }

    //关闭客户端
    public void closeZKClient() {
        if (client != null) {
            this.client.close();
        }
    }

    public static void main(String[] args) throws Exception {
        //实例化
        CuratorOperator cto = new CuratorOperator();
        boolean isZKCuratorStarted = cto.client.isStarted();
        System.out.println("当前客户端的状态："+(isZKCuratorStarted?"连接中":"已关闭"));

        String nodePath= "/fan/super";
        //创建节点

//        byte[] data = "superme".getBytes();
//        cto.client.create().creatingParentsIfNeeded()
//                .withMode(CreateMode.PERSISTENT)
//                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
//                .forPath(nodePath,data );
//
        //更新节点
//        byte[] newData = "batman".getBytes();
//        cto.client.setData().withVersion(2).forPath(nodePath, newData);

        //刪除节点
//        cto.client.delete()
//                .guaranteed()               //如果删除失败，那么在后端还是会继续删除
//                .deletingChildrenIfNeeded() //如果有子节点，就删除
//                .withVersion(0)
//                .forPath(nodePath);

        //读取节点数据
//        Stat stat = new Stat();
//        byte[] data = cto.client.getData().storingStatIn(stat).forPath(nodePath);   //h
//        System.out.println("节点"+nodePath+"的数据为:"+new String(data));
//        System.out.println("该节点的版本号为:"+stat.getVersion());
//

        //查询子节点
//        List<String> childNode = cto.client.getChildren().forPath(nodePath);
//        System.out.println("开始打印子节点");
//        for (String s: childNode) {
//            System.out.println(s);
//        }

        //判断节点是否存在，如果不存在则为空
//        Stat statExist = cto.client.checkExists().forPath(nodePath+"/acd");
//        System.out.println(statExist);


        //watch事件 当使用usingwatcher的时候，监听只会触发一次，监听完毕后就销毁
//        cto.client.getData().usingWatcher(new MyCuratorWatcher()).forPath(nodePath);
//        cto.client.getData().usingWatcher(new MyWatcher()).forPath(nodePath);

        //为节点添加watcher
        //NodeCache:监听数据节点的变更，会触发事件
//        final NodeCache nodeCache = new NodeCache(cto.client, nodePath);
//        //初始化 buidIntial：初始化时候获取 node值并且缓存
//        nodeCache.start(true);
//        if (nodeCache.getCurrentData() != null) {
//            System.out.println("节点初始化数据为:"+new String(nodeCache.getCurrentData().getData()));
//        }else {
//            System.out.println("节点初始化数据为空...");
//        }
//        nodeCache.getListenable().addListener(new NodeCacheListener() {
//            @Override
//            public void nodeChanged() throws Exception {
//                String data = new String(nodeCache.getCurrentData().getData());
//                System.out.println("节点路径："+nodeCache.getCurrentData().getPath()+"数据:"+data);
//            }
//        });

        //为子节点添加Watcher
        //PathChildrenCache :监听数据节点的增删改，会触发事件
        String childNodePathCache = nodePath;
        //cacheData :设置缓存节点的数据状态
        final PathChildrenCache ChildrenCache = new PathChildrenCache(cto.client, childNodePathCache, true);
        /**
         *  StartMode:初始化方式
         *  POST_INITIALIZED_EVENT:异步初始化 ，初始化以后会触发事件
         *  NORMAL:异步初始化
         *  BUILD_INITIAL_CACHE:同步初始化   //可以直接显示子节点
         */

        ChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        List<ChildData> childDataList = ChildrenCache.getCurrentData();
        System.out.println("当前数据节点的子节点数据列表：");
        for (ChildData c :childDataList) {
            String childData = new String(c.getData());
            System.out.println(childData);
        }


        //监听的操作
        ChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                if (event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)){
                    System.out.println("子节点初始化完成...");
                }else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)){
                    System.out.println("添加字节点:"+event.getData().getPath());
                    System.out.println("子节点数据:"+new String(event.getData().getData()));
                }else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)){
                    System.out.println("删除子节点:"+event.getData().getPath());
                }else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)){
                    System.out.println("修改子节点路径:"+event.getData().getPath());
                    System.out.println("修改子节点数据:"+new String(event.getData().getData()));
                }
            }
        });

        Thread.sleep(100000);

        cto.closeZKClient();
        boolean isZKCuratorStarted2 = cto.client.isStarted();
        System.out.println("当前客户端的状态："+( isZKCuratorStarted2?"连接中":"已关闭"));

    }
}