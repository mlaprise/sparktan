"""
Create new sparktan project template.
"""

from __future__ import absolute_import

from sparktan.bootstrap import quickstart

def main(args):
    """ Create new streamparse project template. """
    quickstart(args['<project>'])
