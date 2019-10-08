# MLSQL Console Panel introduction


## Workspace 

![](http://docs.mlsql.tech/upload_images/1063603-ad4b2e91807adf66.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)


![](http://docs.mlsql.tech/upload_images/1063603-906e3cbed89a5828.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

1. Script area, Manage Scripts.
2. Shortcut menu area, by double-clicking or dragging to the editing area to generate mlsql code.
3. Edit area, edit MLSQL script.
4. Command area, the area where scripts are run and saved, and timeout can also be set.
5. Message area, where the run log is output.
6. Tool/data visualization area, specific SQL syntax can trigger the generation of charts. In addition, file upload is also in this area.
7. Table area, the successful scripts are usually displayed in the form of tables.

## Shortcut menu area

![](https://upload-images.jianshu.io/upload_images/1063603-0d31b1725883e423.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## Tool/data visualization area
 
The following code can draw a graph:

```sql
set abc='''
{ "x": 100, "y": 200, "z": 200 ,"dataType":"A group"}
{ "x": 120, "y": 100, "z": 260 ,"dataType":"B group"}
''';
load jsonStr.`abc` as table1;
select x,y,z,dataType,"scatter" as dash,
map("A group",map("shape","star")) as _dash_config 
from table1 as output;
```

After running, click on [Tools/Dashboard]/Dashboard to see the graphics.

![](http://docs.mlsql.tech/upload_images/WX20190819-180034.png)

## Task progress

When running complex tasks, it can monitor resource consumption and running progress in real time.

![](http://docs.mlsql.tech/upload_images/1063603-086e68544b74df9a.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


## Console menu

Other menu selection:

1. Demo Center  
2. Team


### Demo Center 

Demonstrate the capabilities of MLSQL, and of course you can learn grammar as well.

![](http://docs.mlsql.tech/upload_images/1063603-d4453efa16e124b0.png)

You can see the next step by clicking the "Next Step" button, or you can run it on the current page.
### Team
Team provides authority and personnel management functions. We will introduce the permission system in MLSQL Console in detail in the following chapters.
![](http://docs.mlsql.tech/upload_images/WX20190819-180413.png)






