package tech.mlsql.cluster.model;

import net.csdn.jpa.model.Model;

import java.util.List;
import java.util.Map;

/**
 * 2018-12-04 WilliamZhu(allwefantasy@gmail.com)
 */
public class Backend extends Model {


    public static Backend newOne(Map<String, String> params, boolean persist) {
        Backend backend = create(params);
        if (persist) {
            backend.save();
        }
        return backend;
    }

    public static Backend find(int id) {
        return Backend.find(id);
    }

    public static List<Backend> items() {
        return Backend.findAll();
    }

    private String url;
    private String tag;
    private String name;
    private Integer ecsResourcePoolId;

    public int getEcsResourcePoolId() {
        return ecsResourcePoolId;
    }

    public void setEcsResourcePoolId(int ecsResourcePoolId) {
        this.ecsResourcePoolId = ecsResourcePoolId;
    }

    public String getUrl() {
        return url;
    }

    public String getTag() {
        return tag;
    }

    public String[] getTags() {
        if (getTag() != null) {
            return getTag().split(",");
        }
        return new String[]{};
    }

    public String getName() {
        return name;
    }
}
