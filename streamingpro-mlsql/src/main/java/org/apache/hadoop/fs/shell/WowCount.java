/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.shell;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;

import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedList;

/**
 * Count the number of directories, files, bytes, quota, and remaining quota.
 */

public class WowCount extends WowFsCommand {
    public WowCount(Configuration conf, String basePath, PrintStream out, PrintStream error) {
        super(conf, basePath, out, error);
    }

    /**
     * Register the names for the count command
     *
     * @param factory the command factory that will instantiate this class
     */


    private static final String OPTION_QUOTA = "q";
    private static final String OPTION_HUMAN = "h";

    public static final String NAME = "count";
    public static final String USAGE =
            "[-" + OPTION_QUOTA + "] [-" + OPTION_HUMAN + "] <path> ...";
    public static final String DESCRIPTION =
            "Count the number of directories, files and bytes under the paths\n" +
                    "that match the specified file pattern.  The output columns are:\n" +
                    "DIR_COUNT FILE_COUNT CONTENT_SIZE FILE_NAME or\n" +
                    "QUOTA REMAINING_QUOTA SPACE_QUOTA REMAINING_SPACE_QUOTA \n" +
                    "      DIR_COUNT FILE_COUNT CONTENT_SIZE FILE_NAME\n" +
                    "The -h option shows file sizes in human readable format.";

    private boolean showQuotas;
    private boolean humanReadable;


    @Override
    protected void processOptions(LinkedList<String> args) {
        CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE,
                OPTION_QUOTA, OPTION_HUMAN);
        cf.parse(args);
        redefineBaseDir(args);
        if (args.isEmpty()) { // default path is the current working directory
            args.add(this.basePath);
        }
        showQuotas = cf.getOpt(OPTION_QUOTA);
        humanReadable = cf.getOpt(OPTION_HUMAN);
    }

    @Override
    protected void processPath(PathData src) throws IOException {
        ContentSummary summary = src.fs.getContentSummary(src.path);
        out.println(summary.toString(showQuotas, isHumanReadable()) + cleanPath(src.toString()));
    }

    /**
     * Should quotas get shown as part of the report?
     *
     * @return if quotas should be shown then true otherwise false
     */
    @InterfaceAudience.Private
    boolean isShowQuotas() {
        return showQuotas;
    }

    /**
     * Should sizes be shown in human readable format rather than bytes?
     *
     * @return true if human readable format
     */
    @InterfaceAudience.Private
    boolean isHumanReadable() {
        return humanReadable;
    }
}
