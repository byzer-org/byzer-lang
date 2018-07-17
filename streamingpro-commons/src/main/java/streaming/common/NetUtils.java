package streaming.common;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by allwefantasy on 16/7/2018.
 */
public class NetUtils {

    public static List<Integer> getPorts(int number, int MIN_PORT_NUMBER, int MAX_PORT_NUMBER) {
        int start = MIN_PORT_NUMBER + 1;
        boolean stop = false;
        List<Integer> result = new ArrayList<Integer>();
        while (!stop && start < MAX_PORT_NUMBER) {
            if (available(start, MIN_PORT_NUMBER, MAX_PORT_NUMBER)) {
                result.add(start);
                if (result.size() == number) {
                    stop = true;
                }
                start = start + 1;
            }
        }
        if (result.size() != number) {
            throw new IllegalArgumentException("Can not collect enough port in range " + MIN_PORT_NUMBER + " and " + MAX_PORT_NUMBER);
        }
        return result;
    }

    public static String getHost() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static ServerSocket holdPort(int port) {
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ss;
    }

    public static void releasePort(ServerSocket ss) {
        if (ss == null) return;
        try {
            ss.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return;
    }

    public static boolean available(int port, int MIN_PORT_NUMBER, int MAX_PORT_NUMBER) {
        if (port < MIN_PORT_NUMBER || port > MAX_PORT_NUMBER) {
            throw new IllegalArgumentException("Invalid start port: " + port);
        }

        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                /* should not be thrown */
                }
            }
        }

        return false;
    }

    public static ServerSocket availableAndReturn(int MIN_PORT_NUMBER, int MAX_PORT_NUMBER) {
        boolean bindSuccess = false;
        ServerSocket ss = null;
        AtomicInteger start = new AtomicInteger(MIN_PORT_NUMBER);

        while (!bindSuccess && start.get() < MAX_PORT_NUMBER) {
            try {
                ss = new ServerSocket(start.get());
                ss.setReuseAddress(true);
                bindSuccess = true;
            } catch (IOException e) {
                bindSuccess = false;
                start.set(start.get() + 1);
            } finally {
                if (ss != null && !bindSuccess) {
                    try {
                        ss.close();
                    } catch (IOException e) {
                /* should not be thrown */
                    }
                }
            }


        }
        return ss;
    }
}
