""" This is the docstring.
"""
######################################################################
##
def printoutput(outstring, outfile):
    """ This is the docstring.
    """
    print(outstring)
    outfile.write('%s\n' % (outstring))
    outfile.flush()
