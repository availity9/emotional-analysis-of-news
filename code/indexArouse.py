# -*- coding: utf-8 -*-
"""
Created on Wed Dec 19 09:58:55 2018

@author: availity
"""

path = input("input the path:")
index = []
with open(path,"r",encoding = "utf-8") as f:
    for str in f:
        index.append(str[2:8]+"\n")
with open("index.txt","w") as f:
    f.writelines(index)