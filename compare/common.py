#!/usr/bin/env python
#  -*- coding=utf-8 -*-


import os

def get_curdir():
    curdir = os.getcwd()
    rsep = curdir.rfind(os.sep)
    return curdir[:rsep+1]

BASEDIR = get_curdir()

if __name__ == '__main__':
    print get_curdir()
