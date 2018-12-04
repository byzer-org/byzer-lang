package tech.mlsql.cluster.model;

import net.csdn.jpa.model.Model;

import java.util.List;
import java.util.Map;

/**
 * 2018-12-04 WilliamZhu(allwefantasy@gmail.com)
 */
public class Backend extends Model {


    public static Backend newBackend(Map<String, String> params) {
        Backend backend = create(params);
        backend.save();
        return backend;
    }

    public static List<Backend> items() {
        return Backend.findAll();
    }

    private String url;
    private String tag;

    public String getUrl() {
        return url;
    }

    public String getTag() {
        return tag;
    }
}
