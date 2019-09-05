## Overview

MLSQL Console is a Web product that integrates data development, data analysis and machine learning and so on.
Its goal is to have a unified data workbench for products, operations, analysts, research and development, algorithms, etc. 
This article focuses on products and operations. In this article, they will learn how to operate and associate multiple excels  on the platform,At the same time, the results are charted.

## Introducing workspace

![image.png](https://upload-images.jianshu.io/upload_images/1063603-ad4b2e91807adf66.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

![image.png](https://upload-images.jianshu.io/upload_images/1063603-906e3cbed89a5828.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

The shortcut menu area can automatically generate ML SQL statements for us. Generally speaking, users only need to be able to write some select statements manually.

## Processed data description
 
There are two excel files:

![image.png](https://upload-images.jianshu.io/upload_images/1063603-a4268a01aa434433.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

The contents are as follows:

![image.png](https://upload-images.jianshu.io/upload_images/1063603-a214772bdcccdd2b.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

![image.png](https://upload-images.jianshu.io/upload_images/1063603-41a3a88400b58e4a.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

The first excel had the number of patients per department per day. The second excel has the director and the corresponding mailbox.
## Analyzing Task List

Now our goal is:

1. Draw the distribution map of patients received by each department every day,
2. Find the doctor's mailbox with the largest number of patients per day,So it is easy to see the distribution of the number of patients in the two departments.
3. save the analysis result as a new EXCEL and download it to your own computer.


## Task 1
There are four steps:
1. Upload the excel document and download it to your workspace to get the operation path.
2. Take the table name after loading excel file.
3. Using SQL to manipulate Excel data.
4. Use SQL to generate icons

Specific steps:

###  Step1:Upload files
Open the Tools / Dashboard, then drag and drop excel-example (the directory contains two excel examples) to upload in the upload area:
![image.png](https://upload-images.jianshu.io/upload_images/1063603-d4d47c0c84fff34f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

After uploading, drag and drop Quick Menu/Download uploaded file to edit area:
![image.png](https://upload-images.jianshu.io/upload_images/1063603-d03b9fe04355e4b6.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Enter the folder name and the directory you want to save. Click Ok, the system will automatically generate statements, click run, the system will show the actual directory of file downloads:
![image.png](https://upload-images.jianshu.io/upload_images/1063603-f7ef38eb5e7e3f4e.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

So far, the paths of files on remote servers are:

```
/tmp/excel-example/triage-patient.xlsx
/tmp/excel-example/master-email.xlsx
```

### Step2: Load Excel and view
Then load excel and transform it into a table that SQL can operate. Drag "Load data" to the editing area:
![image.png](https://upload-images.jianshu.io/upload_images/1063603-98c548a326a37454.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Fill in the path and the table name (the name is optional). Click Ok to generate the corresponding statement.
Similarly, complete the processing of another script.

By this time, you can view the content through the table name:

![image.png](https://upload-images.jianshu.io/upload_images/1063603-a486d5b7368e9e85.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Excel data can be displayed correctly.

### Step3: Data preprocessing
           
Then we use SQL to draw a polyline map, the abscissa is date, and the ordinate is PatientNum's two curves, which correspond to ophthalmology and dermatology respectively. The corresponding patientNum in ophthalmology is called y1, while the corresponding patientNum in dermatology is called y2. For convenience, first filter dermatological data, then set Y1 to 0, Y2 to the actual number of patients.
Similarly, ophthalmic data were processed. Then put these data together and the final SQL is roughly as follows:

```
select date  as x, 0 as y1, patientNum as y2 from triagePatient where triage="皮肤科"
union all
select date  as x, patientNum as y1, 0 as y2 from triagePatient where triage="眼科" 
as tempTable;
```

### Step4: Generate charts and analysis
```sql
select x,sum(y1) as `眼科`,sum(y2) as `皮肤科`, 
-- 告诉系统需要生成图表
"line" as dash
from tempTable where x is not null group by x order by x asc 
as finalTable;
```

In order to show the graph, the abscissa name must be x, and then the dash parameter tells the system what graph to use for display. The legend is a line chart. The final SQL is probably as follows:
![image.png](https://upload-images.jianshu.io/upload_images/1063603-0c0d1b7ace2909ae.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Click to run and the results are as follows:

![image.png](https://upload-images.jianshu.io/upload_images/1063603-8940cbefc9f48cb9.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Click Tools/Dashboard to see the icon:

![image.png](https://upload-images.jianshu.io/upload_images/1063603-778c4038efc64fe9.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

It can be concluded that the difference between them is very large, and there are still missing data in Department of dermatology.

![image.png](https://upload-images.jianshu.io/upload_images/1063603-fad260b994b3b063.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## Task 2

Since we have finished uploading and loading files, task 2 only needs to do data preprocessing and generate icons.

### Step1: Data preprocessing
The core of the second task is to associate two tables.
Join grammar is used here:

```sql
select tp.*,me.email from triagePatient as tp  left join masterEmail as me on tp.master==me.master
as triagePatientWithEmail;
```

### Step2: Generate charts and analysis

Get a new table with an email field, and aggregate according to the user:

```sql
select first(email) as x, 
avg(patientNum) as patientEveryDay
"bar" as dash
from triagePatientWithEmail 
group by master 
order by patientEveryDay desc
as output;
```

A graph drawn with a histogram, e-mail as abscissa and the average number of patients as ordinate

![image.png](https://upload-images.jianshu.io/upload_images/1063603-637bcd5598fa686f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

The results showed that the average daily visits of "jack@hotmail" department were far ahead.


##  Task 3：Save and download include email data as Excel file
           
Finally, save the triagePatientWithEmail table and download it to your computer. Drag and drop "Save data" to the editing area, open the dialog box, select excel format, and save the triagePatientWithEmail table to / TMP / triagePatientWithEmail. xlsx:

![image.png](https://upload-images.jianshu.io/upload_images/1063603-e1676154ed18233d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Click OK to automatically generate statements, click run, and display the saved results.

![image.png](https://upload-images.jianshu.io/upload_images/1063603-16ad7aabe0b55209.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

It's perfect. Then we download and drag it now.

![image.png](https://upload-images.jianshu.io/upload_images/1063603-f0e0800e53569366.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Fill the path in the editing area:

![image.png](https://upload-images.jianshu.io/upload_images/1063603-d8a48875f679a162.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Click Ok to open a new tab for download.
## Complete script
 
```sql
-- -----------------------------------------------------------------------------
-- 数据描述：
--
-- 我们有两个excel文件，第一个文件是每个科室每天接收的病人，并且有这个科室的负责人。
-- 第二个文件是科室负责人以及对应的email信息。
--
-- 需求描述：
-- 1. 我们希望看到科室每天接收到的人的一个时间分布图。
-- 2. 日均接收用户最高的科室负责人的email
-- ------------------------------------------------------------------------------

-- 需求一

-- 下载文件
-- run command as DownloadExt.`` where 
-- from="excel-example" and
-- to="/tmp";
 
load excel.`/tmp/excel-example/triage-patient.xlsx` where useHeader="true"  as triagePatient;
load excel.`/tmp/excel-example/master-email.xlsx` where useHeader="true" as masterEmail;

-- select date_format(cast (UNIX_TIMESTAMP(date, 'dd/MM/yy') as TIMESTAMP),'dd/MM/yy') as x,date from triagePatient as output;

select date  as x, 0 as y1, patientNum as y2 from triagePatient where triage="皮肤科"
union all
select date  as x, patientNum as y1, 0 as y2 from triagePatient where triage="眼科" 
as tempTable;

select x,sum(y1) as `眼科`,sum(y2) as `皮肤科`, 
-- 告诉系统需要生成图表
"line" as dash
from tempTable where x is not null group by x order by x asc 
as finalTable;

select tp.*,me.email from triagePatient as tp  left join masterEmail as me on tp.master==me.master
as triagePatientWithEmail;

select first(email) as x, 
avg(patientNum) as patientEveryDay,master,first(email) as email, 
"bar" as dash
from triagePatientWithEmail 
group by master 
order by patientEveryDay desc
as output;

save overwrite triagePatientWithEmail as excel.`/tmp/triagePatientWithEmail.xlsx`;

```
