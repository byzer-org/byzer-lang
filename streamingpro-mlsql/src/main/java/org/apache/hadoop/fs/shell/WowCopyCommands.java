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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Various commands for copy files
 */

public class WowCopyCommands {
    public static void registerCommands(CommandFactory factory) {
//        factory.addClass(Merge.class, "-getmerge");
//        factory.addClass(Cp.class, "-cp");

    }

    /**
     * merge multiple files together
     */
    public static class Merge extends WowFsCommand {
        public static final String NAME = "getmerge";
        public static final String USAGE = "[-nl] <src> <localdst>";
        public static final String DESCRIPTION =
                "Get all the files in the directories that " +
                        "match the source file pattern and merge and sort them to only " +
                        "one file on local fs. <src> is kept.\n" +
                        "-nl: Add a newline character at the end of each file.";

        protected PathData dst = null;
        protected String delimiter = null;
        protected List<PathData> srcs = null;

        public Merge(Configuration conf, String basePath, PrintStream out, PrintStream error) {
            super(conf, basePath, out, error);
        }

        @Override
        protected void processOptions(LinkedList<String> args) throws IOException {
            try {
                CommandFormat cf = new CommandFormat(2, Integer.MAX_VALUE, "nl");
                cf.parse(args);
                redefineBaseDir(args);
                delimiter = cf.getOpt("nl") ? "\n" : null;

                dst = new PathData(new URI(args.removeLast()), getConf());
                if (dst.exists && dst.stat.isDirectory()) {
                    throw new PathIsDirectoryException(cleanPath(dst.toString()));
                }
                srcs = new LinkedList<PathData>();

            } catch (URISyntaxException e) {
                throw new IOException("unexpected URISyntaxException", e);
            }
        }

        @Override
        protected void processArguments(LinkedList<PathData> items)
                throws IOException {
            super.processArguments(items);
            if (exitCode != 0) { // check for error collecting paths
                return;
            }
            FSDataOutputStream out = dst.fs.create(dst.path);
            try {
                for (PathData src : srcs) {
                    FSDataInputStream in = src.fs.open(src.path);
                    try {
                        IOUtils.copyBytes(in, out, getConf(), false);
                        if (delimiter != null) {
                            out.write(delimiter.getBytes("UTF-8"));
                        }
                    } finally {
                        in.close();
                    }
                }
            } finally {
                out.close();
            }
        }

        @Override
        protected void processNonexistentPath(PathData item) throws IOException {
            exitCode = 1; // flag that a path is bad
            super.processNonexistentPath(item);
        }

        // this command is handled a bit differently than others.  the paths
        // are batched up instead of actually being processed.  this avoids
        // unnecessarily streaming into the merge file and then encountering
        // a path error that should abort the merge

        @Override
        protected void processPath(PathData src) throws IOException {
            // for directories, recurse one level to get its files, else skip it
            if (src.stat.isDirectory()) {
                if (getDepth() == 0) {
                    recursePath(src);
                } // skip subdirs
            } else {
                srcs.add(src);
            }
        }
    }

    public static class Cp extends WowCommandWithDestination {
        public static final String NAME = "cp";
        public static final String USAGE = "[-f] [-p | -p[topax]] <src> ... <dst>";
        public static final String DESCRIPTION =
                "Copy files that match the file pattern <src> to a " +
                        "destination.  When copying multiple files, the destination " +
                        "must be a directory. Passing -p preserves status " +
                        "[topax] (timestamps, ownership, permission, ACLs, XAttr). " +
                        "If -p is specified with no <arg>, then preserves " +
                        "timestamps, ownership, permission. If -pa is specified, " +
                        "then preserves permission also because ACL is a super-set of " +
                        "permission. Passing -f overwrites the destination if it " +
                        "already exists. raw namespace extended attributes are preserved " +
                        "if (1) they are supported (HDFS only) and, (2) all of the source and " +
                        "target pathnames are in the /.reserved/raw hierarchy. raw namespace " +
                        "xattr preservation is determined solely by the presence (or absence) " +
                        "of the /.reserved/raw prefix and not by the -p option.\n";

        public Cp(Configuration conf, String basePath, PrintStream out, PrintStream error) {
            super(conf, basePath, out, error);
        }

        @Override
        protected void processOptions(LinkedList<String> args) throws IOException {
            popPreserveOption(args);
            CommandFormat cf = new CommandFormat(2, Integer.MAX_VALUE, "f");
            cf.parse(args);
            setOverwrite(cf.getOpt("f"));
            // should have a -r option
            setRecursive(true);
            redefineBaseDir(args);
            getRemoteDestination(args);

        }

        private void popPreserveOption(List<String> args) {
            for (Iterator<String> iter = args.iterator(); iter.hasNext(); ) {
                String cur = iter.next();
                if (cur.equals("--")) {
                    // stop parsing arguments when you see --
                    break;
                } else if (cur.startsWith("-p")) {
                    iter.remove();
                    if (cur.length() == 2) {
                        setPreserve(true);
                    } else {
                        String attributes = cur.substring(2);
                        for (int index = 0; index < attributes.length(); index++) {
                            preserve(FileAttribute.getAttribute(attributes.charAt(index)));
                        }
                    }
                    return;
                }
            }
        }
    }


}
