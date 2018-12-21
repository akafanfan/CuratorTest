package org.fan.curator.checkConfig;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.fan.utils.AclUtils;

import java.util.ArrayList;
import java.util.List;

public class CuratorAcl {
    public CuratorFramework client = null;
    public static final String zkServerPath = "222.24.28.185:2181";

    public CuratorAcl() {
        RetryPolicy retryPolicy = new RetryNTimes(3, 5000);
        client = CuratorFrameworkFactory.builder()
                .connectString(zkServerPath)
                .sessionTimeoutMs(10000).retryPolicy(retryPolicy)
                .namespace("workspace").build();
        client.start();
    }

    public void closeZKClient() {
        if (client != null) {
            this.client.close();
        }
    }

    public static void main(String[] args) throws Exception {
        //实例化
        CuratorAcl cto = new CuratorAcl();
        boolean isZkCuratorStarted = cto.client.isStarted();
        System.out.println("当前客户端状态："+(isZkCuratorStarted?"连接中":"已关闭"));

        String nodePath = "/acl/fan";

        List<ACL> acls = new ArrayList<>();
        Id yang1 = new Id("digest", AclUtils.getDigestUserPwd("yang1:123456"));
        Id yang2 = new Id("digest", AclUtils.getDigestUserPwd("yang2:123123"));
        acls.add(new ACL(ZooDefs.Perms.ALL, yang1));
        acls.add(new ACL(ZooDefs.Perms.READ, yang1));
        acls.add(new ACL(ZooDefs.Perms.DELETE| ZooDefs.Perms.CREATE, yang2));



        //创建节点
        byte[] data = "spuerman".getBytes();
        cto.client.create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(nodePath, data);



    }
}
