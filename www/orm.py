#!/usr/bin/env python3
#-*- coding: utf-8 -*-

__author__='unLuck_J'

import asyncio,aiomysql,logging

def log(sql,args=()):
    logging.info('SQL: %s' %sql)

#create connection pool funcation
async def create_pool(loop, **kw):
    logging.info('Create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host = kw.get('host','localhost'),
        port = kw.get('port', 3306),
        user = kw['user'],
        password = kw['password'],
        db = kw['db'],        
        charset = kw.get('charset','utf8'),
        autocommit = kw.get('autocommit', True),
        maxsize = kw.get('maxsize', 10),
        minsize = kw.get('minsize', 1),
        loop=loop
    )

#sql select 查询功能
async def select(sql,args,size=None):
    log(sql,args)
    global __pool
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or ())   #mysql占位符为%s
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
        logging.info('rows returned: %s' %len(rs))
        return rs

#sql delete,update,insert funcations
async def execute(sql,args,autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?','%s'), args)
                affeceted = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affeceted

def create_args_string(num):
    l = []
    for n in range(num):
        l.append('?')
    return ','.join(l)    

#defined Field class
class Field(object):
    
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    
    def __str__(self):
        return '<%s:%s>'%(self.__class__.__name__, self.name)

class StringField(Field):
    def __init__(self,name=None, primary_key=False, default=None, ddl='varchar100'):
        super(StringField,self).__init__(name,ddl,primary_key,default)

class BooleanField(Field):
    def __init__(self,name=None, default=False):
        super(BooleanField,self).__init__(name,'boolean',False,default)

class IntegerField(Field):
    def __init__(self,name=None, primary_key=False, default=0):
        super(IntegerField,self).__init__(name,'bigint',primary_key,default)

class FloatField(Field):
    def __init__(self,name=None, primary_key=False, default=0.0):
        super(FloatField,self).__init__(name,'real',primary_key,default)

class TextField(Field):
    def __init__(self,name=None, default=None):
        super(TextField,self).__init__(name,'text',False,default)

#defined metaclass
class ModelMetaClass(type):
    def __new__(cls,name,bases,attrs):
        if name=='Model':
            return type.__new__(cls,name,bases,attrs)
        tableName = attrs.get('__table__',None) or name
        logging.info('found model:%s (table:%s)'%(name,tableName))
        mappings = dict()
        fields=[]
        primary_key = None
        for k,v in attrs.items():
            if isinstance(v,Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    if primary_key:
                        raise Exception('Duplicate primary key for field: %s' % k)
                    primary_key = k
                else:
                    fields.append(k)
        if not primary_key:
            raise Exception('Primary_key not found')

        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primary_key # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primary_key, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primary_key, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primary_key)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primary_key)
        return type.__new__(cls, name, bases, attrs)

class Model(dict,metaclass=ModelMetaClass):
    def __init__(self,**kw):
        super(Model,self).__init__(kw)

    def __getattr__(self,key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute: '%s'" %key)

    def __setattr__(self,key,value):
        self[key] = value

    def getValue(self,key):
        return getattr(self,key,None)

    def getValueOrDefault(self,key):
        value = getattr(self,key,None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' %(key,str(value)))
                setattr(self,key,value)

        return value

    @classmethod
    async def findAll(cls,where=None,args=None,**kw):
        'find object by where clause.'
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args=[]
        orderBy = kw.get('OrderBy',None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit',None)
        if limit is None:
            sql.append('limit')
            if isinstance(limit,int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit,tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value:%s' %str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args= None):
        'find number by select and where'
        sql = ['select count(%s) from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls,pk):
        'find object by primary key'
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key,), [pk], 1)    
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    #object save to db function
    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    #object update to db function
    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update record: affected rows: %s' % rows)

    #object remove to db function
    async def remove(self):
        args = list(self.getValue(self.__primary_key__))
        rows = await execute(self.__delte__, args)
        if rows != 1:
            logging.warn('failed to delete record: affected rows: %s' % rows)