# Design and Principle

After completing the model training with MLSQL, we must be eager to deploy the model and provide API services.
Usually, there are three usage scenarios:

1. Batch processing. For example, a unified prediction of historical data.
2. streaming calculation. Deploy the model in a streaming program.
3. API services. Model prediction service is provided through API. (this is one of the most common forms).
 
All Feature Engineering ET (Estimator/Transformer) can be registered as UDF functions of mlsql platform, and all models can also be registered as UDF functions.
So as long as API Server supports the registration of these functions, we can complete an end-to-end prediction service through the combination of these functions.


The following is the schematic diagram

```


Training phase    Text Collection   ------  TF/IDF vector(TFIDFInplace) ----- 随机森林(RandomForest) 

                                              |                           |
Output                                      model                       model
                                              |                           |
                                           register                    register
                                              |                           |  
Forecasting services       single Text     -------     udf1     ---------------     udf2   ---> Forecasting result
```

All model generated during the training phase can be fully registered by API Server and easy to implement end-to-end prediction.


            