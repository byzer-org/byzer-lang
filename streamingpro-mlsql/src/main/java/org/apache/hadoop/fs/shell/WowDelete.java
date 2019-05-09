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
import org.apache.hadoop.fs.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Classes that delete paths
 */

public class WowDelete {

    /**
     * remove non-directory paths
     */
    public static class Rm extends WowFsCommand {
        public static final String NAME = "rm";
        public static final String USAGE = "[-f] [-r|-R] [-skipTrash] <src> ...";
        public static final String DESCRIPTION =
                "Delete all files that match the specified file pattern. " +
                        "Equivalent to the Unix command \"rm <src>\"\n" +
                        "-skipTrash: option bypasses trash, if enabled, and immediately " +
                        "deletes <src>\n" +
                        "-f: If the file does not exist, do not display a diagnostic " +
                        "message or modify the exit status to reflect an error.\n" +
                        "-[rR]:  Recursively deletes directories";

        private boolean skipTrash = false;
        private boolean deleteDirs = false;
        private boolean ignoreFNF = false;

        public Rm(Configuration conf, String basePath, PrintStream out, PrintStream error) {
            super(conf, basePath, out, error);
        }

        @Override
        protected void processOptions(LinkedList<String> args) throws IOException {
            CommandFormat cf = new CommandFormat(
                    1, Integer.MAX_VALUE, "f", "r", "R", "skipTrash");
            cf.parse(args);
            ignoreFNF = cf.getOpt("f");
            deleteDirs = cf.getOpt("r") || cf.getOpt("R");
            skipTrash = cf.getOpt("skipTrash");

            redefineBaseDir(args);
        }

        @Override
        protected List<PathData> expandArgument(String arg) throws IOException {
            try {
                return super.expandArgument(arg);
            } catch (PathNotFoundException e) {
                if (!ignoreFNF) {
                    throw e;
                }
                // prevent -f on a non-existent glob from failing
                return new LinkedList<PathData>();
            }
        }

        @Override
        protected void processNonexistentPath(PathData item) throws IOException {
            if (!ignoreFNF) super.processNonexistentPath(item);
        }

        @Override
        protected void processPath(PathData item) throws IOException {
            if (item.stat.isDirectory() && !deleteDirs) {
                throw new PathIsDirectoryException(item.toString());
            }

            // TODO: if the user wants the trash to be used but there is any
            // problem (ie. creating the trash dir, moving the item to be deleted,
            // etc), then the path will just be deleted because moveToTrash returns
            // false and it falls thru to fs.delete.  this doesn't seem right
            if (moveToTrash(item)) {
                return;
            }
            if (!item.fs.delete(item.path, deleteDirs)) {
                throw new PathIOException(item.toString());
            }
            out.println("Deleted " + cleanPath(item.toString()));
        }

        private boolean moveToTrash(PathData item) throws IOException {
            boolean success = false;
            if (!skipTrash) {
                try {
                    success = Trash.moveToAppropriateTrash(item.fs, item.path, getConf());
                } catch (FileNotFoundException fnfe) {
                    throw fnfe;
                } catch (IOException ioe) {
                    String msg = ioe.getMessage();
                    if (ioe.getCause() != null) {
                        msg += ": " + ioe.getCause().getMessage();
                    }
                    throw new IOException(msg + ". Consider using -skipTrash option", ioe);
                }
            }
            return success;
        }
    }

    /**
     * remove any path
     */
    public static class Rmr extends Rm {
        public static final String NAME = "rmr";

        public Rmr(Configuration conf, String basePath, PrintStream out, PrintStream error) {
            super(conf, basePath, out, error);
        }

        @Override
        protected void processOptions(LinkedList<String> args) throws IOException {
            args.addFirst("-r");
            super.processOptions(args);
        }

        @Override
        public String getReplacementCommand() {
            return "rm -r";
        }
    }

    /**
     * remove only empty directories
     */
    public static class Rmdir extends WowFsCommand {
        public static final String NAME = "rmdir";
        public static final String USAGE =
                "[--ignore-fail-on-non-empty] <dir> ...";
        public static final String DESCRIPTION =
                "Removes the directory entry specified by each directory argument, " +
                        "provided it is empty.\n";

        private boolean ignoreNonEmpty = false;

        public Rmdir(Configuration conf, String basePath, PrintStream out, PrintStream error) {
            super(conf, basePath, out, error);
        }

        @Override
        protected void processOptions(LinkedList<String> args) throws IOException {
            CommandFormat cf = new CommandFormat(
                    1, Integer.MAX_VALUE, "-ignore-fail-on-non-empty");
            cf.parse(args);
            ignoreNonEmpty = cf.getOpt("-ignore-fail-on-non-empty");
            redefineBaseDir(args);
        }

        @Override
        protected void processPath(PathData item) throws IOException {
            if (!item.stat.isDirectory()) {
                throw new PathIsNotDirectoryException(item.toString());
            }
            if (item.fs.listStatus(item.path).length == 0) {
                if (!item.fs.delete(item.path, false)) {
                    throw new PathIOException(item.toString());
                }
            } else if (!ignoreNonEmpty) {
                throw new PathIsNotEmptyDirectoryException(item.toString());
            }
        }
    }

}
