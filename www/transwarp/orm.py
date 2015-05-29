#! /usr/bin/env python
# -*- coding: utf-8 -*-


__author__ = 'Michael Liao'
__student__ = 'Matthew Han'

'''
Database operation module. This module is independent with web module.
'''

import time, logging

import db

class Field(object):

    #类级别的属性，而非实例级别！
    _count = 0

    def __init__(self, **kw):
        self.name = kw.get('name', None)
        self._default = kw.get('default', None)
        self.primary_key = kw.get('primary_key', False)
        self.nullable = kw.get('nullable', False)
        self.updatable = kw.get('updatable', True)
        self.insertable = kw.get('insertable', True)
        self.ddl = kw.get('ddl','')
        self._order = Field._count
        Field._count = Field._count + 1
    #@property的作用  #将方法变为只读的属性；产生@method.setter，将方法变为可写的属性
    @property
    def default(self):
        d = self._default
        #default有可能被赋值为方法，因而写成了判断句式
        return d() if callable(d) else d

    def __str__(self):
        # self.__class__.__name__！  #可以通过dir(class)的方式查看，类中的属性和方法；self.__class__.__name__应该是默认的类名
        s = ['<%s:%s,%s,default(%s),' % (self.__class__.__name__, self.name, self.ddl, self._default)]
        #注意这种写法
        self.nullable and s.append('N')
        self.updatable and s.append('U')
        self.insertable and s.append('I')
        s.append('>')
        #str.join(list)！   #将字符串列表中的元素合并在一起
        return ''.join(s)

#思考以下几个子类的设定！   #以下几个类的不同本质上在于default和ddl
class StringField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = ''
        #关于ddl！  #待深入研究
        if not 'ddl' in kw:
            kw['ddl'] = 'varchar(255)'
        super(StringField, self).__init__(**kw)

class IntegerField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = 0
        if not 'ddl' in kw:
            kw['ddl'] = 'bigint'
        super(IntegerField, self).__init__(**kw)

class FloatField(Field):

    def __init_(self, **kw):
        if not 'default' in kw:
            kw['default'] = 0.0
        if not 'ddl' in kw:
            kw['ddl'] = 'real'
        super(FloatField, self).__init__(**kw)

class BooleanField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = False
        if not 'ddl' in kw:
            kw['ddl'] = 'bool'
        super(BooleanField, self).__init__(**kw)

class TextField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'text'
        super(TextField, self).__init__(**kw)

class BlobField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'blob'
        super(BlobFiled, self).__init__(**kw)

class VersionField(Field):

    def __init__(self, name=None):
        super(VersionField, self).__init__(name=name, default=0, ddl='bigint')

#frozenset！   #frozenset是固定的，有散列值的，因而可以用于字典的键或者其他set的元素；set则正好相反。
_triggers = frozenset(['pre_insert', 'pre_update', 'predelet'])

#这个函数只是为生成sql提供文本解释        
def _gen_sql(table_name, mappings):
    #pk应该是primary key的意思？
    pk = None
    sql = ['-- generating SQL for %s:' % table_name, 'create table ‘%s‘ (' %table_name]
    #cmp！      #cmp用于比较两个对象，根据结果返回-1,0,1
    #mappings.values()？
    for f in sorted(mappings.values(), lambda x,y: cmp(x._order, y._order)):
        if not hasattr(f, 'ddl'):
            raise StandardError('no ddl in field "%s".' % n)
        ddl = f.ddl
        nullable = f.nullable
        if f.primary_key:
            pk = f.name
        sql.append(nullable and ' ‘%s‘ %s,' % (f.name, ddl) or ' ‘%s‘ %s not null,' % (f.name, ddl))
    sql.append(' primary key(‘%s‘)' % pk)
    sql.append(');')
    return '\n'.join(sql)

#type是什么类！  #这里涉及元类的概念,结合相关章节理解
class ModelMetaclass(type):
    '''
    Metaclass for model objects.
    '''
    def __new__(cls, name, bases, attrs):
        #skip base Model class:        
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)

        #store all subclasses info:
        if not hasattr(cls, 'subclasses')
            cls.subclasses = {}
        if not name in cls.subclasses:
            cls.subclasses[name] = name
        else:
            logging.warning('Redefine class: %s' % name)

        logging.info('Scan ORMapping %s...' % name)
        mappings = dict()
        primary_key = None
        for k, v in attrs.iteritems():
            if isinstance(v, Field):
                if not v.name:
                    v.name = k
                logging.info('Found mapping: %s => %s' % (k,v))
                #check duplicate primary key:
                if v.primary_key:
                    if primary_key:
                        raise TypeError('Cannot define more than 1 primary key in class: %s' % name)
                    if v.updatable:
                        logging.warning('NOTE: change primary key to non-updatable')
                        v.updatable = False
                    if v.nullable:
                        logging.warning('NOTE: change primary key to non-nullable.')
                        v.nullable = False
                    primary_key = v
                mappings[k] = v
            #check exist of primary key:
            if not primary_key:
                raise TypeError('Primary key not defined in class: %s' % name)
            for k in mappings.iterkeys():
                attrs.pop(k)
            if not '__table__' in attrs:
                attrs['__table__'] = name.lower()
            attrs['__mappings__'] = mappings
            attrs['__primary_key__'] = primary_key
            attrs['__sql__'] = lambda self: _gen_sql(attrs['__table__'], mappings)
            for trigger in _triggers:
                if not trigger in attrs:
                    attrs[trigger] = None
            return type.__new__(cls, name, bases, attrs)

class Model(dict):
    '''
    Base class for ORM.
    >>> class User(Model):
    ...     id = IntegerField(primary_key=True)
    ...     name = StringField()
    ...     email = StringField(updatable=False)
    ...     passwd = StringField(default=lambda: '******')
    ...     last_modified = FloatField()
    ...     def pre_insert(self):
    ...         self.last_modified = time.time()
    >>> u = User(id=10190, name='Michael', email='orm@db.org')
    >>> r = u.insert()
    >>> u.email
    'orm@db.org'
    >>> u.passwd
    '******'
    >>> u.last_modified > (time.time() - 2)
    True
    >>> f = User.get(10190)
    >>> f.name
    u'Michael'
    >>> f.email
    u'orm@db.org'
    >>> f.email = 'changed@db.org'
    >>> r = f.update() # change email but email is non-updatable!
    >>> len(User.find_all())
    1
    >>> g = User.get(10190)
    >>> g.email
    u'orm@db.org'
    >>> r = g.delete()
    >>> len(db.select('select * from user where id=10190'))
    0
    >>> import json
    >>> print User().__sql__()
    -- generating SQL for user:
    create table `user` (
      `id` bigint not null,
      `name` varchar(255) not null,
      `email` varchar(255) not null,
      `passwd` varchar(255) not null,
      `last_modified` real not null,
      primary key(`id`)
    );
    '''

    __metaclass__ = ModelMetaclass

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    #@classmethod!   #该装饰器装饰的方法可以直接被类（而非实例）调用，因而也意味着方法不能包含self参数，取而代之的是cls。
    @classmethod
    def get(cls, pk):
        '''
        Get by primary key.
        '''
        d = db.select_one('select * from %s where %s=?' % (cls.__table__, cls.__primary_key__.name), pk)
        return cls(**d) if d else None

    @classmethod
    def find_first(cls, where, *args):
        '''
        Find by where clause and return one result. If multiple results found,
        only the first one returned. If not result found, return None.
        '''
        d = db.select_one('select * from %s %s' % (cls.__table__, where), *args)
        return cls(**d) if d else None

    @classmethod
    def count_all(cls):
        '''
        Find by 'select count(pk) from table' and return integer.
        '''
        return db.select_int('select count(‘%s‘) from ‘%s‘' % (cls.__primary_key__.name, cls.__table__))

    @classmethod
    def count_by(cls, where, *args):
        '''
        Find by 'select count(pk) from table where...' and return int.
        '''
        return db.select_int('select count(‘%s‘) from ‘%s‘ %s' % (cls.__primary_key__.name, cls.__table__, where), *args)
    
    def update(self):
        #self.pre_update和self.pre_update()？
        self.pre_update and self.pre_update()
        L = []
        args = []
        #self.__mappings__？
        for k, v in self.__mappings__.iteritems():
            if v.updatable:
                if hasattr(self, k):
                    arg = getattr(self, k)
                else:
                    arg = v.default
                    setattr(self, k, arg)
                    L.append('‘%s‘=?' % k)
                    args.append(arg)
            pk = self.__primary_key__.name
            args.append(getattr(self, pk))
            db.update('update ‘%s‘ set %s where %s=?' % (self.__table__,','.join(L), pk), *args)
            return self

    def delete(self):
        #self.pre_delete和self.pre_delete()？
        self.pre_delete and self.pre_delete()
        pk = self.__primary_key__.name
        args = (getattr(self, pk),)
        db.update('delete from ‘%s‘ where ‘%s‘=?' % (self.__table__, pk), *args)
        return self

    def insert(self):
        #self.pre_insert和self.pre_insert()？
        self.pre_insert and self.pre_insert()
        params = {}
        for k, v in self.__mappings__.iteritems():
            if v.insertable:
                if not hasattr(self, k):
                    setattr(self, k, v.default)
                params[v.name] = getattr(self, k)
        db.insert('%s' % self.__table__, **params)
        return self

    if __name__ == '__main__':
        logging.basicConfig(level=logging.DEBUG)
        db.create_engine('www-data', 'www-data', 'test')
        db.update('drop table if exists user')
        db.update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
        import doctest
        doctest.testmod()
        
        
                    
    
