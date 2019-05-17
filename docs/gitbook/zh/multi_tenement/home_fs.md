# 主目录管理

MLSQL Engine提供了一套兼容hdfs命令行的指令集，可以方便你查看目录，删除文件，创建目录，拷贝移动文件等。
使用示例如下：

```sql
!hdfs -ls /tmp /;
!hdfs -rm -r /tmp/sample_libsvm_data.txt;
!hdfs -count -h /tmp;
!hdfs -cp /tmp/triagePatientWithEmail.xlsx /tmp/triagePatientWithEmail2.xlsx;
```

你也可以使用 `!fs` 替代 `!hdfs`。 两者之间是等价的。 

