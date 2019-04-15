# 加载图片数据

其实MLSQL内置了好几个图片处理ET(Esitmator/Transformer),不过我们今天只会接受 ImageLoaderExt.

我们来看一段示例：

```sql

select 1 as a as emptyData;

set imageDir="/Users/allwefantasy/Downloads/jack";

run emptyData as ImageLoaderExt.`${imageDir}`
where code='''
        def apply(params:Map[String,String]) = {
         Resize(256, 256) -> CenterCrop(224, 224) ->
          MatToTensor() -> ImageFrameToSample()
       }
''' as images;
select imageName from images limit 1 as output;
```

ImageLoaderExt支持传递Scala处理代码对图片进行处理。我们看看里面的代码片段,首先方法是apply,这个是默认的，而且是必须。其次里面的params参数其实是where条件里的其他参数。

接着方法主体是一个DSL:

```scala
Resize(256, 256) -> CenterCrop(224, 224) -> MatToTensor() -> ImageFrameToSample()
```

对每张图片做了四个操作：

1. 把图片缩放为256*256大小
2. 进行裁切为224*224
3. 图片转化为张量
4. 最后转化为后续深度学习可以高效处理的结构（其实就是Image DataFrame）


> 通常加载和处理图片是一个比较慢的过程

其中3,4两部分每次你都带上就好。那我还有哪些可以操作呢？

```scala
Brightness(deltaLow: Double, deltaHigh: Double)
Hue(deltaLow: Double, deltaHigh: Double)
Saturation(deltaLow: Double, deltaHigh: Double)
Contrast(deltaLow: Double, deltaHigh: Double)
ChannelOrder() //随机修改图片channel顺序

ColorJitter(brightnessProb: Double = 0.5,
                              brightnessDelta: Double = 32,
                              contrastProb: Double = 0.5,
                              contrastLower: Double = 0.5,
                              contrastUpper: Double = 1.5,
                              hueProb: Double = 0.5,
                              hueDelta: Double = 18,
                              saturationProb: Double = 0.5,
                              saturationLower: Double = 0.5,
                              saturationUpper: Double = 1.5,
                              randomOrderProb: Double = 0,
                              shuffle: Boolean = false)

Resize(resizeH: Int, resizeW: Int,
                    resizeMode: Int = Imgproc.INTER_LINEAR,
                    useScaleFactor: Boolean = true)

AspectScale(scale: Int, scaleMultipleOf: Int = 1,
                    maxSize: Int = 1000)

/*
image channel normalize

meanR mean value in R channel
meanG mean value in G channel
meanB mean value in B channel
stdR std value in R channel
stdG std value in G channel
stdB std value in B channel                    
 */
ChannelNormalize(meanR: Float, meanG: Float, meanB: Float,
                                         stdR: Float = 1, stdG: Float = 1, stdB: Float = 1) 
                                         
PixelNormalizer(means: Array[Float])  
CenterCrop(cropWidth: Int, cropHeight: Int, isClip: Boolean = true)
RandomCrop(cropWidth: Int, cropHeight: Int, isClip: Boolean = true)
FixedCrop(x1: Float, y1: Float, x2: Float, y2: Float, normalized: Boolean,
                      isClip: Boolean = true)
DetectionCrop(roiKey: String, normalized: Boolean = true)  

/*
expand image, fill the blank part with the meanR, meanG, meanB

meansR means in R channel
meansG means in G channel
meansB means in B channel
minExpandRatio min expand ratio
maxExpandRatio max expand ratio                    
 */
Expand(meansR: Int = 123, meansG: Int = 117, meansB: Int = 104,
                    minExpandRatio: Double = 1, maxExpandRatio: Double = 4.0)
                    

/*
Fill part of image with certain pixel value

startX start x ratio
startY start y ratio
endX end x ratio
endY end y ratio
value filling value                    
 */
Filler(startX: Float, startY: Float, endX: Float, endY: Float, value: Int = 255)

HFlip() 

RandomTransformer(transformer: FeatureTransformer, maxProb: Double)                                                                             
```
