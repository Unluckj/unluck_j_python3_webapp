#!/usr/bin/env python3
#-*- coding: utf-8 -*-

__author__='unLuck_J'

import config_default

class Dict(dict):
    '''
    Simple dict bust supoort as x.y style
    '''

    def __init__(self,names=(), values=(), **kw):
        super(Dict,self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self,k):
        try:
            return self[k]
        except KeyError:
            raise AttributeError('no %s attribute'% k)

    def __setattr__(self,k,v):
        self[k] = v

def merge(default, override):
    r = {}
    for k,v in default.items():
        if k in override:
            r[k] = merge(v,override[k]) if isinstance(v,dict) else override[k]
        else:
            r[k] = v
    return r   

def toDict(d):
    D = {}
    for k,v in d.items():
        D[k] = toDict(v) if isinstance(v,dict) else v 
    return D

configs = config_default.configs

try:
    import config_overide
    overrides = config_overide.configs
    configs = merge(configs,overrides)
except ImportError:
    pass

configs = toDict(configs)
#print(configs)

    