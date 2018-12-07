package tech.mlsql.cluster.model;

import net.csdn.common.collections.WowCollections;
import net.csdn.jpa.model.Model;

import java.util.List;
import java.util.Map;

/**
 * 2018-12-05 WilliamZhu(allwefantasy@gmail.com)
 */
public class EcsResourcePool extends Model {
    public static EcsResourcePool newOne(Map<String, String> params, boolean persist) {
        EcsResourcePool backend = create(params);
        backend.setInUse(NOT_IN_USE);
        if (persist) {
            backend.save();
        }
        return backend;
    }

    public static EcsResourcePool find(int id) {
        return EcsResourcePool.find(id);
    }

    public static List<EcsResourcePool> items() {
        return EcsResourcePool.findAll();
    }

    public static List<String> requiredFields() {
        return WowCollections.list("ip", "loginUser", "name", "keyPath", "sparkHome", "mlsqlConfig");
    }

    public static String IN_USE = "in_use";
    public static String NOT_IN_USE = "not_in_use";

    private String ip;
    private String keyPath;
    private String loginUser;
    private String name;
    private String sparkHome;
    private String mlsqlHome;
    private String mlsqlConfig;
    private String executeUser;
    private String tag;
    private String inUse;

    public void setInUse(String inUse) {
        this.inUse = inUse;
    }

    public String getInUse() {
        return inUse;
    }

    public String getMlsqlHome() {
        return mlsqlHome;
    }

    public String getTag() {
        return tag;
    }

    public String getExecuteUser() {
        if (executeUser == null) return loginUser;
        return executeUser;
    }

    public String getIp() {
        return ip;
    }

    public String getKeyPath() {
        return keyPath;
    }

    public String getLoginUser() {
        return loginUser;
    }

    public String getName() {
        return name;
    }

    public String getSparkHome() {
        return sparkHome;
    }

    public String getMlsqlConfig() {
        return mlsqlConfig;
    }

}
