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

package tech.mlsql.tool;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import static tech.mlsql.tool.OrderedProperties.loadPropertiesFromInputStream;

public class ByzerConfig {

    private static final Logger log = LoggerFactory.getLogger(ByzerConfig.class);

    public final static ByzerConfig INSTANCE = new ByzerConfig();

    private Properties properties = new Properties();

    private static final String PROPERTIES_DIR = "conf";

    private static final String PROPERTIES_FILE = "byzer.properties";

    private static final String OVERRIDE_PROPERTIES_FILE = "byzer.properties.override";

    private ByzerConfig() {
        loadConfig();
    }

    public static ByzerConfig getInstance() {
        return INSTANCE;
    }

    public void loadConfig() {
        Properties conf = new Properties();
        OrderedProperties orderedProperties = buildOrderedProps();
        for (Map.Entry<String, String> each : orderedProperties.entrySet()) {
            conf.put(each.getKey(), each.getValue());
        }
        this.properties = conf;
    }

    private static OrderedProperties buildOrderedProps() {

        try {
            OrderedProperties orderedProperties = new OrderedProperties();
            // 1. load base conf, named byzer.properties
            File propFile = getPropertiesFile();
            if (propFile == null || !propFile.exists()) {
                log.error("fail to locate " + PROPERTIES_FILE);
                throw new RuntimeException("fail to locate " + PROPERTIES_FILE);
            }
            loadAndTrimProperties(new FileInputStream(propFile), orderedProperties);

            // 2. support byzer.properties.override as override file
            File propOverrideFile = new File(propFile.getParentFile(), OVERRIDE_PROPERTIES_FILE);
            if (propOverrideFile.exists()) {
                loadAndTrimProperties(new FileInputStream(propOverrideFile), orderedProperties);
            }
            return orderedProperties;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void loadAndTrimProperties(InputStream inputStream, OrderedProperties properties) {
        Preconditions.checkNotNull(inputStream);
        Preconditions.checkNotNull(properties);
        try {
            OrderedProperties trimProps = OrderedProperties.copyAndTrim(loadPropertiesFromInputStream(inputStream));
            properties.putAll(trimProps);
        } catch (Exception e) {
            log.error(" loadAndTrimProperties error ", e);
            throw new RuntimeException(" loadAndTrimProperties error ", e);
        }
    }

    static File getPropertiesFile() {
        String path = getPropertiesDirPath();
        return existFile(path);
    }

    private static File existFile(String path) {
        if (path == null) {
            return null;
        }

        return new File(path, PROPERTIES_FILE);
    }

    public String getOptional(String propertyKey, String defaultValue) {
        String property = System.getProperty(propertyKey);
        if (!StringUtils.isBlank(property)) {
            return property.trim();
        }
        property = properties.getProperty(propertyKey);
        if (StringUtils.isBlank(property)) {
            return defaultValue.trim();
        } else {
            return property.trim();
        }
    }

    public Properties getProperties() {
        return properties;
    }

    public static String getPropertiesDirPath() {
        return getByzerHome() + File.separator + PROPERTIES_DIR;
    }

    public static String getByzerHome() {
        String byzerHome = System.getProperty("BYZER_HOME");
        if (StringUtils.isBlank(byzerHome)) {
            byzerHome = System.getenv("BYZER_HOME");
            if (StringUtils.isBlank(byzerHome)) {
                throw new RuntimeException("BYZER_HOME Not Set");
            }
        }
        return byzerHome;
    }

}
