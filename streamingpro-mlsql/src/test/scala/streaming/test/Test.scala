package streaming.test

/**
  * 2019-03-05 WilliamZhu(allwefantasy@gmail.com)
  */
object Test {
  def main(args: Array[String]): Unit = {
    val a = "![image.png](https://upload-images.jianshu.io/upload_images/1063603-8c7b735d2377e045.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)".split("upload_images/").last.split("\\?").head
    println(a)
  }
}
