package streaming.test;

import streaming.core.StreamingApp;

public class MyTest {
    public static void main(String[] args) {
        StreamingApp.main(new String[] {"-streaming.name user_regter_test_6",
        "-streaming.platform flink_streaming",
        "-streaming.job.file.path file://C:\\Users\\jiguang\\Desktop\\test_data\\flink_user_register.json"});
    }

}
