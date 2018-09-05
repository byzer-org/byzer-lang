package streaming.common.http;

/**
 * Created by zhuml on 2018/9/3.
 */
public class HttpResult {

    // 响应码
    private int code;

    // 响应体
    private String body;

    // 提示信息
    private String message;

    public HttpResult() {
        super();
    }

    public HttpResult(int code, String body) {
        super();
        this.code = code;
        this.body = body;
    }

    public HttpResult(int code, String body, String message) {
        super();
        this.code = code;
        this.body = body;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
