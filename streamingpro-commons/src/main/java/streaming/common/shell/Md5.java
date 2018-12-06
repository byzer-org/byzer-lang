package streaming.common.shell;

import java.security.MessageDigest;

/**
 * Created by allwefantasy on 12/7/2017.
 */
public class Md5 {
    public Md5() {
    }

    public static final String MD5(String s) {
        try {
            byte[] e = s.getBytes();
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            mdInst.update(e);
            byte[] md = mdInst.digest();
            StringBuffer sb = new StringBuffer();

            for(int i = 0; i < md.length; ++i) {
                int val = md[i] & 255;
                if(val < 16) {
                    sb.append("0");
                }

                sb.append(Integer.toHexString(val));
            }

            return sb.toString();
        } catch (Exception var7) {
            return null;
        }
    }

    public static void main(String[] args) {
        String err = "e,3,4,";
        System.out.println(err.substring(0, err.length() - 1));
    }
}
