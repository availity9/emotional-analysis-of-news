# -*- coding: utf-8 -*-
"""
Created on Wed Dec 19 10:17:48 2018

@author: availity
"""

import pandas as pd
import numpy as np
import random
import matplotlib.pyplot as plt

index = [] #index
lab1 = {} #out of KNN
lab2 = {} #out of naive Bayes

#get index and KNN
with open("exp4/availity/out_afterKNN/part-r-00000","r",encoding = "utf-8") as f:
    for str in f:
        index.append(int(str[2:8]))
        lab1[int(str[2:8])] = str.split("\t")[1][:-1]
        
#get naive Bayes
with open("exp4/availity/out_naive/part-r-00000","r",encoding = "utf-8") as f:
    for str in f:
        lab2[int(str[2:8])] = str.split("\t")[1][:-1]
        
#get average return in 9-10 month
df = pd.DataFrame(pd.read_excel("data.xlsx"))
lab3 = {}
#print(df)
for i in range(0,len(index)): #initial
    lab3[index[i]] = 0
for j in range(0, len(df)): #sum
    if(np.isnan(df.iloc[j,5])):
        continue
    lab3[df.iloc[j,1]] += df.iloc[j,5]
for i in range(0,len(index)): #average
    lab3[index[i]] /= 3
#print(lab3)

#classification
lab4 = {}
for i in range(0,len(index)):
    if(abs(lab3[index[i]]) <0.01):
        lab4[index[i]] = "neutral"
    elif(lab3[index[i]] > 0):
        lab4[index[i]] = "positive"
    else:
        lab4[index[i]] = "negative"
#print(lab4)

#plot KNN (just include positive and negative)
TN = 0
TP = 0
FP = 0
FN = 0
plt.title("KNN plot")
plt.xlabel("label")
plt.ylabel("label")
for i in range(0,len(index)):
    if(lab1[index[i]] == "positive" and lab4[index[i]] == "positive"):
        TP += 1
        first = random.random()
        second = random.random()
        plt.scatter(first,second, s = 20, c = 'b',marker='o')
    elif(lab1[index[i]] == "positive" and lab4[index[i]] == "negative"):
        FN += 1
        first = random.random()
        second = random.random()*(-1)
        plt.scatter(first,second, s = 20, c = 'g',marker='o')
    elif(lab1[index[i]] == "negative" and lab4[index[i]] == "positive"):
        FP += 1
        first = random.random()*(-1)
        second = random.random()
        plt.scatter(first,second, s = 20, c = 'y',marker='o')
    elif(lab1[index[i]] == "negative" and lab4[index[i]] == "negative"):
        TN += 1
        first = random.random()*(-1)
        second = random.random()*(-1)
        plt.scatter(first,second, s = 20, c = 'r',marker='o')
    else:
        continue
plt.show()
AM = (TN+TP)/(TN+FP+FN+TP)
F1 = 2*TP/(2*TP+FP+FN)
print("AM is: ",AM)
print("F1 is: ",F1)

#plot naive (just include positive and negative)
TN = 0
TP = 0
FP = 0
FN = 0
plt.title("Naive Bayes plot")
plt.xlabel("label")
plt.ylabel("label")
for i in range(0,len(index)):
    if(lab2[index[i]] == "positive" and lab4[index[i]] == "positive"):
        TP += 1
        first = random.random()
        second = random.random()
        plt.scatter(first,second, s = 20, c = "blue",marker='o')
    elif(lab2[index[i]] == "positive" and lab4[index[i]] == "negative"):
        FN += 1
        first = random.random()
        second = random.random()*(-1)
        plt.scatter(first,second, s = 20, c = "green",marker='o')
    elif(lab2[index[i]] == "negative" and lab4[index[i]] == "positive"):
        FP += 1
        first = random.random()*(-1)
        second = random.random()
        plt.scatter(first,second, s = 20, c = "yellow",marker='o')
    elif(lab2[index[i]] == "negative" and lab4[index[i]] == "negative"):
        TN += 1
        first = random.random()*(-1)
        second = random.random()*(-1)
        plt.scatter(first,second, s = 20, c = "red",marker='o')
    else:
        continue
plt.show()
AM = (TN+TP)/(TN+FP+FN+TP)
F1 = 2*TP/(2*TP+FP+FN)
print("AM is: ",AM)
print("F1 is: ",F1)