package com.krux.kafka.offsetReporter;

import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ConsumerOffsetReporter {

    public static void main(String[] args) {

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy);
        client.start();

        try {
            List<String> children = client.getChildren().forPath("/");
            for (String child : children) {
                System.out.println(child);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        client.close();

    }

}
