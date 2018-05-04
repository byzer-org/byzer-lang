package streaming.common;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by allwefantasy on 4/5/2018.
 */
public class UnicodeUtils {

    public static String toUnicode(String s) {
        String as[] = new String[s.length()];
        String s1 = "";
        for (int i = 0; i < s.length(); i++) {
            as[i] = Integer.toHexString(s.charAt(i) & 0xffff);
            s1 = s1 + "\\u" + as[i];
        }
        return s1;
    }

    public static String keepChinese(String chineseStr, boolean keepPunctuation, String[] include) {
        if (chineseStr == null) return null;
        Set<Character> includeChars = new HashSet<>();

        if (include != null) {
            for (String item : include) {
                includeChars.add(item.charAt(0));
            }
        }

        StringBuffer buffer = new StringBuffer();
        char[] charArray = chineseStr.toCharArray();
        for (int i = 0; i < charArray.length; i++) {
            if ((charArray[i] >= 0x4e00) && (charArray[i] <= 0x9fbb)) {
                buffer.append(charArray[i]);
            }
            if (keepPunctuation && PunctuationUtils.isPunctuation(charArray[i])) {
                buffer.append(charArray[i]);
            }
            if (includeChars.size() > 0 && includeChars.contains(charArray[i])) {
                buffer.append(charArray[i]);
            }
        }
        return buffer.toString();
    }
}
