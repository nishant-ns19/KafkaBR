package com.nishant.kafkabr.utils;

import java.util.ArrayList;
import java.util.List;

public class MyUtils {
    public static final String PRODUCER_TASK_COLLECTION = "ProducerTask";
    public static final String CONSUMER_GROUP_COLLECTION = "ConsumerGroups";
    public static final String PRODUCER_TASK_ASSIGNED = "assigned";
    public static final String PRODUCER_TASK_ID = "id";
    public static final String CONSUMER_GROUPS_ID = "groupId";
    public static final String CONSUMER_GROUPS_BOOTSTRAP_SERVERS = "bootstrapServers";
    public static final String CONSUMER_GROUPS_TOPIC = "topic";
    public static final String CONSUMER_GROUPS_USERNAME = "username";
    public static final String CONSUMER_GROUPS_PASSWORD = "password";

    public static List<List<String>> partition(List<String> list, int n) {
        int size = list.size();
        int m = size / n;
        if (size % n != 0)
            m++;
        List<List<String>> partition = new ArrayList<>();
        for (int i = 0; i < m; i++) {
            int fromIndex = i * n;
            int toIndex = Math.min(i * n + n, size);

            partition.add(new ArrayList<>(list.subList(fromIndex, toIndex)));
        }
        return partition;
    }

}
