# Cifar10示例

这个章节我们会通过操作Cifar10来演示如何

## 图片准备

首先你要下载图片集，网络上大部分都不是原生图片了，都是已经转化为向量的。大家可以通过这个链接

[cifar.tgz](https://github.com/allwefantasy/spark-deep-learning-toy/releases/download/v0.01/cifar.tgz)

获得图片。

解压后应该有两个目录，一个是train(约6万张)，一个是test.这次示例我们只用train目录下的。

## 加载图片

上一个章节我们已经掌握了如何加载图片数据。也就是通过如下代码，我们已经获得了图片：

```sql
set labelMappingPath = "/tmp/si";
set imageConvertPath = "/tmp/cifar_train_data";
set modelPath = "/tmp/cifar-model";

select 1 as a as emptyData;

set imageDir="/Users/allwefantasy/Downloads/jack";

run emptyData as ImageLoaderExt.`${imageDir}`
where code='''
        def apply(params:Map[String,String]) = {
         Resize(256, 256) -> CenterCrop(224, 224) ->
          MatToTensor() -> ImageFrameToSample()
       }
''' as images;
```

## 抽取分类

通常，我们需要对图片路径也进行处理，这样才好得到分类。这可以通过如下方式：

```
-- convert image path to number label
select split(split(imageName,"_")[1],"\\.")[0] as labelStr,features from data as newdata;
train newdata as StringIndex.`${labelMappingPath}` where inputCol="labelStr" and outputCol="labelIndex" as newdata1;
predict newdata as StringIndex.`${labelMappingPath}` as newdata2;
select (cast(labelIndex as int) + 1) as label,features from newdata2 as newdata3;
```

## 保存处理好的图片数据，方便下次训练时使用

```sql
-- save image processing result.                   
save overwrite newdata3 as parquet.`${imageConvertPath}`;
```

## 训练

````sql
load parquet.`${imageConvertPath}` as newdata3;
select array(cast(label as float)) as label,features from newdata3 as newdata4;

--train with LeNet5 model
train newdata4 as BigDLClassifyExt.`${modelPath}` where
fitParam.0.featureSize="[3,28,28]"
and fitParam.0.classNum="10"
and fitParam.0.maxEpoch="50"
and fitParam.0.code='''
                   def apply(params:Map[String,String])={
                        val model = Sequential()
                        model.add(Reshape(Array(3, 28, 28), inputShape = Shape(28, 28, 3)))
                        model.add(Convolution2D(6, 5, 5, activation = "tanh").setName("conv1_5x5"))
                        model.add(MaxPooling2D())
                        model.add(Convolution2D(12, 5, 5, activation = "tanh").setName("conv2_5x5"))
                        model.add(MaxPooling2D())
                        model.add(Flatten())
                        model.add(Dense(100, activation = "tanh").setName("fc1"))
                        model.add(Dense(params("classNum").toInt, activation = "softmax").setName("fc2"))
                    }
'''
;
````

和其他内置的普通算法一样，我们可以配置多组参数从而进行多组参数的训练。

1. featureSize, 描述张量形状，通道在前
2. classNum 分类数目， cifar10是10个分类
3. maxEpoch进行多少个epoch
4. code DSL 部分，该部分主要进行模型的构建。


和数据加载一样，它也是一段基于scala语法的DSL,可以让你以Keras的形态描述网络结构。
在这里我们构建一个leNet网络。

训练时间会比较长。我们建议修改spark日志，将org.spark.spark相关的日志都设置成WARN.因为日志太多了。

## 批预测

```sql
predict newdata4 as BigDLClassifyExt.`${modelPath}` as predictdata;
```

> 预测结果的分类数字需要+1才是正确的分类。

## API预测

```sql
-- deploy with api server
register BigDLClassifyExt.`/tmp/bigdl` as cifarPredict;

select
vec_argmax(cifarPredict(vec_dense(features))) as predict_label,
label from data
as output;
```




