## AST python抽象语法树


visitor访问抽象语法树的类型，通过```method = 'visit_' + node.__class__.__name__```拿到需要访问的节点类型。所有的类型如下

```
visit_Module
visit_Import
visit_alias
visit_ImportFrom
visit_FunctionDef
visit_arguments
visit_Expr
visit_Call
visit_Name
visit_Load
visit_Attribute
visit_Str
visit_Assign
visit_Store
visit_Print
visit_BinOp
visit_Mod
visit_Num
visit_While
visit_Compare
visit_LtE
visit_AugAssign
visit_Add
visit_TryExcept
visit_keyword
visit_List
visit_If
visit_Eq
visit_Tuple
visit_Sub
visit_Return
visit_ExceptHandler
visit_Raise
visit_Break
visit_Continue
visit_TryFinally
visit_ListComp
visit_comprehension
visit_Lt
visit_In
visit_Subscript
visit_Index
visit_GtE
```

python代码：
```
y = "000123456000"
def add(arg1, arg2):
    x = 1234568888
    y = "000123456000"
    arg1 = arg1 + 1234569999
    return arg1 + arg2    
def func():
    return ""
class MyClass:
    def myFunc():
        import os
        dirs = os.listdir("")
        for d in dirs:
            print(d)
    def myFunc2(x):
        return 666
myc = MyClass()
myc.myFunc()
myc.myFunc2(2)
```
编译成的抽象语法树如下：
```
Module(
    body=[
        Assign(
            lineno=2,
            col_offset=0,
            targets=[Name(lineno=2, col_offset=0, id='y', ctx=Store())],
            value=Str(lineno=2, col_offset=4, s='000123456000'),
        ),
        FunctionDef(
            lineno=3,
            col_offset=0,
            name='add',
            args=arguments(
                args=[
                    Name(lineno=3, col_offset=8, id='arg1', ctx=Param()),
                    Name(lineno=3, col_offset=14, id='arg2', ctx=Param()),
                ],
                vararg=None,
                kwarg=None,
                defaults=[],
            ),
            body=[
                Assign(
                    lineno=4,
                    col_offset=4,
                    targets=[Name(lineno=4, col_offset=4, id='x', ctx=Store())],
                    value=Num(lineno=4, col_offset=8, n=1234568888),
                ),
                Assign(
                    lineno=5,
                    col_offset=4,
                    targets=[Name(lineno=5, col_offset=4, id='y', ctx=Store())],
                    value=Str(lineno=5, col_offset=8, s='000123456000'),
                ),
                Assign(
                    lineno=6,
                    col_offset=4,
                    targets=[Name(lineno=6, col_offset=4, id='arg1', ctx=Store())],
                    value=BinOp(
                        lineno=6,
                        col_offset=11,
                        left=Name(lineno=6, col_offset=11, id='arg1', ctx=Load()),
                        op=Add(),
                        right=Num(lineno=6, col_offset=18, n=1234569999),
                    ),
                ),
                Return(
                    lineno=7,
                    col_offset=4,
                    value=BinOp(
                        lineno=7,
                        col_offset=11,
                        left=Name(lineno=7, col_offset=11, id='arg1', ctx=Load()),
                        op=Add(),
                        right=Name(lineno=7, col_offset=18, id='arg2', ctx=Load()),
                    ),
                ),
            ],
            decorator_list=[],
        ),
        FunctionDef(
            lineno=8,
            col_offset=0,
            name='func',
            args=arguments(args=[], vararg=None, kwarg=None, defaults=[]),
            body=[
                Return(
                    lineno=9,
                    col_offset=4,
                    value=Str(lineno=9, col_offset=11, s=''),
                ),
            ],
            decorator_list=[],
        ),
        ClassDef(
            lineno=10,
            col_offset=0,
            name='MyClass',
            bases=[],
            body=[
                FunctionDef(
                    lineno=11,
                    col_offset=4,
                    name='myFunc',
                    args=arguments(args=[], vararg=None, kwarg=None, defaults=[]),
                    body=[
                        Import(
                            lineno=12,
                            col_offset=8,
                            names=[alias(name='os', asname=None)],
                        ),
                        Assign(
                            lineno=13,
                            col_offset=8,
                            targets=[Name(lineno=13, col_offset=8, id='dirs', ctx=Store())],
                            value=Call(
                                lineno=13,
                                col_offset=15,
                                func=Attribute(
                                    lineno=13,
                                    col_offset=15,
                                    value=Name(lineno=13, col_offset=15, id='os', ctx=Load()),
                                    attr='listdir',
                                    ctx=Load(),
                                ),
                                args=[Str(lineno=13, col_offset=26, s='')],
                                keywords=[],
                                starargs=None,
                                kwargs=None,
                            ),
                        ),
                        For(
                            lineno=14,
                            col_offset=8,
                            target=Name(lineno=14, col_offset=12, id='d', ctx=Store()),
                            iter=Name(lineno=14, col_offset=17, id='dirs', ctx=Load()),
                            body=[
                                Print(
                                    lineno=15,
                                    col_offset=12,
                                    dest=None,
                                    values=[Name(lineno=15, col_offset=18, id='d', ctx=Load())],
                                    nl=True,
                                ),
                            ],
                            orelse=[],
                        ),
                    ],
                    decorator_list=[],
                ),
                FunctionDef(
                    lineno=16,
                    col_offset=4,
                    name='myFunc2',
                    args=arguments(
                        args=[Name(lineno=16, col_offset=16, id='x', ctx=Param())],
                        vararg=None,
                        kwarg=None,
                        defaults=[],
                    ),
                    body=[
                        Return(
                            lineno=17,
                            col_offset=8,
                            value=Num(lineno=17, col_offset=15, n=666),
                        ),
                    ],
                    decorator_list=[],
                ),
            ],
            decorator_list=[],
        ),
        Assign(
            lineno=18,
            col_offset=0,
            targets=[Name(lineno=18, col_offset=0, id='myc', ctx=Store())],
            value=Call(
                lineno=18,
                col_offset=6,
                func=Name(lineno=18, col_offset=6, id='MyClass', ctx=Load()),
                args=[],
                keywords=[],
                starargs=None,
                kwargs=None,
            ),
        ),
        Expr(
            lineno=19,
            col_offset=0,
            value=Call(
                lineno=19,
                col_offset=0,
                func=Attribute(
                    lineno=19,
                    col_offset=0,
                    value=Name(lineno=19, col_offset=0, id='myc', ctx=Load()),
                    attr='myFunc',
                    ctx=Load(),
                ),
                args=[],
                keywords=[],
                starargs=None,
                kwargs=None,
            ),
        ),
        Expr(
            lineno=20,
            col_offset=0,
            value=Call(
                lineno=20,
                col_offset=0,
                func=Attribute(
                    lineno=20,
                    col_offset=0,
                    value=Name(lineno=20, col_offset=0, id='myc', ctx=Load()),
                    attr='myFunc2',
                    ctx=Load(),
                ),
                args=[Num(lineno=20, col_offset=12, n=2)],
                keywords=[],
                starargs=None,
                kwargs=None,
            ),
        ),
    ],
)
```