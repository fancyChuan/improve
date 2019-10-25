# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2019/10/24 17:14
@Contact:  1247375074@qq.com 
@Software: PyCharm
"""
import os
import ast
import astpretty
import codegen

expr = """
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
"""

# with open("bi_otherdata_transform.py", "r") as f:
#     expr = f.read()

expr_ast = ast.parse(expr)


class MyVisitor(ast.NodeVisitor):

    def __init__(self):
        super(MyVisitor, self).__init__()

    def visit(self, node):
        """Visit a node."""
        method = 'visit_' + node.__class__.__name__
        print("[ast]" + method)
        visitor = getattr(self, method, self.generic_visit)
        return visitor(node)

    # def visit_Str(self, node):
    #     print('Found string "%s"', node.s)
    #     if node.s == "12345":
    #         node.s = "45678"

    # def visit_FunctionDef(self, node):
    #     print('Found function: ', node._fields)

    def visit_Name(self, node):
        """Name类型的节点有三种属性"""
        print("visit name: ", node.id, node.ctx)

    # def visit_Module(self, node):
    #     """
    #         Module类型的节点只有body一个属性，显示整个py文件整体结构情况
    #         比如上面的代码得到的body如下：
    #         [<_ast.Assign at 0x3abc550>,
    #          <_ast.FunctionDef at 0x3abc5f8>,
    #          <_ast.FunctionDef at 0x3abca20>,
    #          <_ast.ClassDef at 0x3abcb00>,
    #          <_ast.Assign at 0x3abcf98>,
    #          <_ast.Expr at 0x3b7a0b8>,
    #          <_ast.Expr at 0x3b7a198>]
    #
    #     """
    #     return self.visit(node.body[0])

    def visit_Assign(self, node):
        node.id


astpretty.pprint(expr_ast)

MyVisitor().visit(expr_ast)

# codegen.to_source(expr_ast)