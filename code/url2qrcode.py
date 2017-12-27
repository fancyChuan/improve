# -*- encoding: utf-8 -*-
"""
Created on 2:06 AM 7/30/2017

@author: fancyChuan
"""

import qrcode

img = qrcode.make('http://115.28.105.44/admin/')
img.save('../doc/admin_login.png')


img = qrcode.make('hhttp://115.28.105.44/member_login')
img.save('../doc/member_login.png')