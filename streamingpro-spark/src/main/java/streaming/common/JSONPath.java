package streaming.common;

import com.jayway.jsonpath.Predicate;
import com.jayway.jsonpath.internal.JsonContext;

/**
 * 6/13/16 WilliamZhu(allwefantasy@gmail.com)
 */
public class JSONPath
{
    public JSONPath() {}

    public static <T> T read(String json, String jsonPath)
    {
        return new JsonContext().parse(json).read(jsonPath, new Predicate[0]);
    }
}
