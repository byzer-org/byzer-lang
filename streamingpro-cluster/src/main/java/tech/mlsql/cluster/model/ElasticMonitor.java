package tech.mlsql.cluster.model;

import net.csdn.common.collections.WowCollections;
import net.csdn.jpa.model.Model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 2018-12-05 WilliamZhu(allwefantasy@gmail.com)
 */
public class ElasticMonitor extends Model {

    public static ElasticMonitor newOne(Map<String, String> params) {
        Map<String, Object> newParams = new HashMap<>();
        params.entrySet().forEach((a) -> {
            if (a.getKey() == "minInstances" || a.getKey() == "maxInstances") {
                newParams.put(a.getKey(), Integer.parseInt(a.getValue()));
            } else newParams.put(a.getKey(), a.getValue());
        });
        ElasticMonitor backend = create(newParams);
        backend.save();
        return backend;
    }

    public static ElasticMonitor find(int id) {
        return ElasticMonitor.find(id);
    }

    public static List<ElasticMonitor> items() {
        return ElasticMonitor.findAll();
    }

    public static List<String> requiredFields() {
        return WowCollections.list("tag", "name", "minInstances", "maxInstances", "allocateType", "allocateStrategy");
    }

    private String tag;
    private String name;
    private int minInstances;
    private int maxInstances;
    private String allocateType;
    private String allocateStrategy;

    public String getTag() {
        return tag;
    }

    public String getName() {
        return name;
    }

    public int getMinInstances() {
        return minInstances;
    }

    public int getMaxInstances() {
        return maxInstances;
    }

    public String getAllocateType() {
        return allocateType;
    }

    public String getAllocateStrategy() {
        return allocateStrategy;
    }

}
