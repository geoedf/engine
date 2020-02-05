#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Simple custom Exception class for all GeoEDF errors
"""

class GeoEDFError(BaseException):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
