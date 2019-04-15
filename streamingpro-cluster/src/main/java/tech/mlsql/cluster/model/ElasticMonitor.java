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

import net.csdn.common.collections.WowCollections;
import net.csdn.jpa.model.Model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 2018-12-05 WilliamZhu(allwefantasy@gmail.com)
 */
public class ElasticMonitor extends Model {

    public static ElasticMonitor newOne(Map<String, String> params, boolean persist) {
        Map<String, Object> newParams = new HashMap<>();
        params.entrySet().forEach((a) -> {
            if (a.getKey() == "minInstances" || a.getKey() == "maxInstances") {
                newParams.put(a.getKey(), Integer.parseInt(a.getValue()));
            } else newParams.put(a.getKey(), a.getValue());
        });
        ElasticMonitor backend = create(newParams);
        if (persist) {
            backend.save();
        }
        return backend;
    }

    public static ElasticMonitor findById(int id) {
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
