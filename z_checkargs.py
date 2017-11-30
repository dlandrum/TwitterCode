""" This is the docstring.
"""
######################################################################
##
import sys
######################################################################
## CHECK ARGUMENTS
def checkargs(number, message):
    """ This is the docstring.
    """
    if len(sys.argv) != number+1:
        print(message)
        sys.exit(1)
