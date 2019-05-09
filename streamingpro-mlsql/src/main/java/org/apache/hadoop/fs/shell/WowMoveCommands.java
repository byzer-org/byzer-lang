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
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIOException;

import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedList;

/**
 * Various commands for moving files
 */

public class WowMoveCommands {


    /**
     * move/rename paths on the same fileystem
     */
    public static class Rename extends WowCommandWithDestination {
        public static final String NAME = "mv";
        public static final String USAGE = "<src> ... <dst>";
        public static final String DESCRIPTION =
                "Move files that match the specified file pattern <src> " +
                        "to a destination <dst>.  When moving multiple files, the " +
                        "destination must be a directory.";

        public Rename(Configuration conf, String basePath, PrintStream out, PrintStream error) {
            super(conf, basePath, out, error);
        }

        @Override
        protected void processOptions(LinkedList<String> args) throws IOException {
            CommandFormat cf = new CommandFormat(2, Integer.MAX_VALUE);
            cf.parse(args);
            redefineBaseDir(args);
            getRemoteDestination(args);
        }

        @Override
        protected void processPath(PathData src, PathData target) throws IOException {
            if (!src.fs.getUri().equals(target.fs.getUri())) {
                throw new PathIOException(cleanPath(src.toString()),
                        "Does not match target filesystem");
            }
            if (target.exists) {
                throw new PathExistsException(cleanPath(target.toString()));
            }
            if (!target.fs.rename(src.path, target.path)) {
                // we have no way to know the actual error...
                throw new PathIOException(cleanPath(src.toString()));
            }
        }
    }
}
