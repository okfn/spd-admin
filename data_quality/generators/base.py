# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals


class BaseGenerator(object):
    """This is the base class for generators. All generators should inherit."""

    def __init__(self, url=None):

        self.base_url = url
        if not self.base_url:
            raise TypeError('Cannot generate the database without the "url" parameter.')

    def generate_sources(self, sources_filepath, file_types=['csv','excel']):
        """Generate sources file for CSV database"""

    def generate_publishers(self, publishers_filepath):
        """Generate publishers file for CSV database"""
