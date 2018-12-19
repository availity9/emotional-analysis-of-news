# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 16:55:41 2018

@author: availity
"""
# tag all txt and put them into a same dictionary

import os

path = "training_data"
filelist = os.listdir(path) # all files in the path
os.mkdir("training") # new path
for files in filelist:
    d = os.path.join(path,files)
    describe = os.path.basename(d) # tag
    sub_filelist = os.listdir(d)
    for sub_files in sub_filelist:
        old_dir = os.path.join(d,sub_files)
        filename = os.path.splitext(sub_files)[0]
        filetype = os.path.splitext(sub_files)[1]
        new_dir = os.path.join("training",filename+"#"+describe+filetype)
        os.rename(old_dir,new_dir)