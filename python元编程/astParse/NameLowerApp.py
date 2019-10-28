# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2019/10/28 11:41
@Contact:  1247375074@qq.com 
需求：
    重新编译代码，重写AST将全局访问变量降为在函数体范围内访问
"""
import ast
import inspect
import codegen


class NameLower(ast.NodeVisitor):
    def __init__(self, lowered_name):
        self.lowered_name = lowered_name

    def visit_FunctionDef(self, node):
        # 拿到运行时环境的全局命名空间
        code = "__globals = globals()\n"
        # 把需要降级的全局变量“实例化”出来。 比如  INCR = __globals['INCR']
        code += "\n".join("{0} = __globals['{0}']".format(name) for name in self.lowered_name)

        code_ast = ast.parse(code, mode="exec")
        # 把前面降级的代码插入到方法体，插入到原有代码的前面
        # 这里使用node.body[:0]的原因是body也是一个list，但是我们需要插入到最前面 list[:0]得到空列表[]
        node.body[:0] = code_ast.body
        # 查看修改后的源码
        print(codegen.to_source(node))
        self.func = node


def lower_names(*namelist):
    def lower(func):
        # 拿到使用了装饰器的函数的完整代码
        sources = inspect.getsource(func)
        print(sources)
        srclines = sources.splitlines()
        for n, line in enumerate(srclines):
            if "@lower_names" in line:
                break
        # 取出装饰器之后的所有代码，这部分代码是需要改造的
        src = "\n".join(srclines[n+1:])
        # Hack to deal with indented code
        if src.startswith((' ', '\t')):
            src = 'if 1:\n' + src
        top = ast.parse(src, mode='exec')

        # Transform the AST
        cl = NameLower(namelist)
        cl.visit(top)

        # Execute the modified AST
        temp = {}
        exec (compile(top, '', 'exec'), temp, temp)  # todo：有什么作用？应该是把top代码所涉及的全局变量和局部变量汇总到temp中

        # Pull out the modified code object
        func.__code__ = temp[func.__name__].__code__
        return func

    return lower

# ============ test code ==============
INCR = 1
@lower_names('INCR')
def countdown(n):
    while n > 0:
        print("now n: %d" % n)
        n -= INCR


if __name__ == '__main__':
    countdown(6)