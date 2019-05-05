### JavaImage

JavaImage is a image library implemented by Java. You should enable it with profile opencv-support.



具体用法：

```sql

select crawler_request_image("https://tpc.googlesyndication.com/simgad/10310202961328364833") as imagePath
as  images;


load image.`/training_set`
options

recursive="true"

and dropImageFailures="true"

and sampleRatio="1.0"

and numPartitions="8"

and repartitionNum="4"

and filterByteSize="2048576"

and enableDecode = "false"
as images;



-- select image.origin,image.width from images
-- as newimages;

train images as JavaImage.`/tmp/word2vecinplace`
where inputCol="imagePath"
and filterByteSize="2048576"

and shape="100,100,4"
-- scale method，default：AUTOMATIC
and method="SPEED"
-- scale mode，default：FIT_EXACT
and mode="AUTOMATIC"
;

load parquet.`/tmp/word2vecinplace/data`
as imagesWithResize;

select vec_image(imagePath) as feature from imagesWithResize
as newTable;
```

Parameters:

|parameter|default|comments|
|:----|:----|:----|
|method|AUTOMATIC|缩放方法|
|mode|FIT_EXACT|缩放模式|
|shape|none|width,height,channel，for example：100,100,3。channel will not work for now.|

method：

|值|说明|
|:----|:----|
|AUTOMATIC|自动,用于表明缩放的实现应该决定使用以获得最佳的期待在最少的时间缩放图像的|
|BALANCED|平衡,用于表明缩放的实现应该使用缩放操作的速度和质量之间的平衡|
|QUALITY|质量,用于表明缩放的实现应该尽其所能创造很好的效果越好|
|SPEED|用于表明缩放的实现的规模应该尽可能快并返回结果|
|ULTRA_QUALITY|用于表明缩放的实现应该超越的质量所做的工作，使图像看起来特别好的更多的处理时间成本|

mode：

|值|说明|
|:----|:----|
|AUTOMATIC|自动,用于表明缩放的实现应该计算所得的图像尺寸，通过查看图像的方向和发电比例尺寸，最佳为目标的宽度和高度，看到“给出更详细的scalr类描述图像的比例”|
|BEST_FIT_BOTH|最佳模式,用于表明缩放的实现应该计算，适合在包围盒的最大尺寸的图片，没有种植或失真，保持原来的比例|
|FIT_EXACT|精准模式,用适合的图像给不顾形象的比例精确的尺寸|
|FIT_TO_HEIGHT|用于表明缩放的实现应该计算尺寸的图像，最适合在给定的高度，无论图像的方向|
|FIT_TO_WIDTH|用于表明缩放的实现应该计算尺寸的图像，最适合在给定的宽度，无论图像的方向|

Image schema:

```scala
StructType(
    StructField("origin", StringType, true) ::
      StructField("height", IntegerType, false) ::
      StructField("width", IntegerType, false) ::
      StructField("nChannels", IntegerType, false) ::
      StructField("mode", StringType, false) ::
      StructField("data", BinaryType, false) :: Nil)
```


Register:

```sql
register JavaImage.`/tmp/word2vecinplace/` as jack;
```

Process:

```sql
select jack(crawler_request_image(imagePath)) as image from orginal_text_corpus
```