package org.apache.hadoop.fs.shell;

import org.apache.hadoop.conf.Configuration;

/**
 * 2019-05-08 WilliamZhu(allwefantasy@gmail.com)
 */
public class WowCommandFactory extends CommandFactory {


    /**
     * Factory constructor for commands
     */
    public WowCommandFactory() {
        this(null);
    }

    /**
     * Factory constructor for commands
     *
     * @param conf the hadoop configuration
     */
    public WowCommandFactory(Configuration conf) {
        super(conf);
    }
}
