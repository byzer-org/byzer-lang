/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.mlsql.cluster.model;

import net.csdn.jpa.model.Model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.csdn.common.collections.WowCollections.map;

/**
 * 2018-12-04 WilliamZhu(allwefantasy@gmail.com)
 */
public class Backend extends Model {


    public static Backend newOne(Map<String, String> params, boolean persist) {
        Map<String, Object> newParams = new HashMap<>();
        params.entrySet().forEach((a) -> {
            if (a.getKey() != "ecsResourcePoolId") {
                newParams.put(a.getKey(), a.getValue());
            }
        });

        Backend backend = Backend.create(newParams);
        if (!params.containsKey("ecsResourcePoolId")) {
            backend.setEcsResourcePoolId(-1);
        } else {
            backend.setEcsResourcePoolId(Integer.parseInt(params.get("ecsResourcePoolId")));
        }
        if (persist) {
            backend.save();
        }
        return backend;
    }

    public static Backend findById(int id) {
        return Backend.find(id);
    }

    public static Backend findByName(String name) {
        return Backend.where(map("name", name)).singleFetch();
    }

    public static List<Backend> items() {
        return Backend.findAll();
    }

    private String url;
    private String tag;
    private String name;
    private Integer ecsResourcePoolId;

    public Integer getEcsResourcePoolId() {
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
