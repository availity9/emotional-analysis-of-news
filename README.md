# emotional-analysis-of-news

### 实验内容
股市新闻中往往包含了大量信息，除了上市公司的财务数据外，还包括经营公告、行业动向、国家政策等大量文本信息，这些文本信息中常常包含了一定的情感倾向，会影响股民对公司股票未来走势的预期，进一步造成公司的股价波动。如果能够挖掘出这些新闻中蕴含的情感信息，则可以对股票价格进行预测，对于指导投资有很大的作用。
本实验尝试使用文本挖掘技术和机器学习算法，挖掘出新闻中蕴含的情感信息，分别将每条新闻的情感判别为“positive”、“neutral”、“negative”这三种情感中的一种，可根据抓取的所有新闻的情感汇总分析来对股票价格做预测。

### 实验环境
1. MapReduce编程环境：
Jdk 1.8
Hadoop 2.9.1（伪分布运行模式）
Eclipse luna
2. Python编程环境：
Spyder

### 实验数据
1. fulldata.txt：收集沪市和深市若干支股票在某时间段内的若干条财经新闻标题；
2. training_data.zip：训练数据集，由保存在negative、neutral和positive三个文件夹下的样本数据组成，文件夹名即其中样本的分类；
3. chi_words.txt：特证词文件，将其中的词语作为特征，并计算特征向量；
4. data.xlsx：沪市和深市若干支股票在2018年9-10月的收益率等数据，从www.ccerdata.cn下载得到；

### 实验步骤
1. 预处理阶段：
- rename.py：处理training_data，将标签加入到样本数据名中，并把样本数据放在同一个文件夹下；
- processTest项目（借鉴wordcount的思想）：处理fulldata.txt，将属于同一股票的新闻标题连在一起；
2. 计算tf-idf：
- calculateIDF项目（借鉴invertedindex和wordcount思想）：计算chi_words.txt中各词的idf值;
- calculateTFIDF项目：计算训练集各文件和测试集各项各词语的tf-idf;
3. 向量化：
- processData项目：将训练集和测试集的tf-idf向量化;
4. 分类：
- classKNN项目：用KNN对测试集进行分类;
- naiveBayes项目：用朴素贝叶斯对测试集进行分类;
5. 评估：
- indexArouse.py：生成股票代码文件，供下载数据使用;
- test.py：评估分类效果并可视化;

### 仓库介绍
- code文件夹下，是实验步骤中所有项目的源代码和可执行文件
- output文件夹下，是实验的所有中间结果和最终运行结果
- word文档，是实验报告

**各项目的输入格式以及代码运行各步骤的情况可参考实验报告中的描述**

**文件目录存在不符 需要自行修改**
