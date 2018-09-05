package streaming.common.http;

import org.apache.commons.lang.RandomStringUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by zhuml on 2018/9/3.
 */
public class OpenApiUtil {

    public static String getNonceStr() {
        return RandomStringUtils.randomAlphanumeric(16);
    }

    public static String getTimestamp() {
        return String.valueOf(System.currentTimeMillis() / 1000);
    }

    public static String createSHA1Sign(TreeMap<String, Object> parameters, String appSignKey) {
        parameters.put("appSignKey", appSignKey);
        StringBuffer sb = new StringBuffer();
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            if (sb.length() > 0) {
                sb.append('&');
            }
            sb.append(entry.getKey());
            sb.append('=');
            sb.append(String.valueOf(entry.getValue()));
        }
        parameters.remove("appSignKey");
        return sha1(sb.toString());
    }

    public static String sha1(String str) {
        byte[] data = str.getBytes();
        if (data != null && data.length > 0) {
            try {
                return new String(toHex(MessageDigest.getInstance("SHA-1").digest(data)));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private static String toHex(byte[] bytes) {
        if (bytes != null && bytes.length > 0) {
            StringBuilder buff = new StringBuilder(bytes.length << 1);
            String tmp = null;
            for (int i = 0; i < bytes.length; i++) {
                tmp = (Integer.toHexString(bytes[i] & 0xFF));
                if (tmp.length() == 1) {
                    buff.append('0');
                }
                buff.append(tmp);
            }
            return buff.toString();
        }
        return null;
    }

    private static boolean checkSign(TreeMap<String, Object> map, String appSignKey, String sign) {
        if (sign.equals(createSHA1Sign(map, appSignKey))) {
            return true;
        }
        return false;
    }
}
