"""
MP4 Swift - Example for Swift project to enable pseudo-streaming for a MP4

@author: Young Kim (ykim@crunchyroll.com)
"""
import sys

# Version Check, Python 2.7 is required; can be Python 2.6
# if different command line reader is used
if sys.hexversion < 0x02070000:
    print "Error: Python 2.7 is required to run this script."
    sys.exit(1)
    
from argparse import ArgumentParser, RawDescriptionHelpFormatter
# Main package for handling conversion
from mp4stream.StreamMp4 import *

# Debug functions below
def print_atoms(parent, indent=0):
    for atom in parent.get_atoms():
        print "%s %s - %d %d" % (("-" * indent), atom.type, atom.size, atom.offset)
        print_atoms(atom, indent=indent+1)

def print_flat(parent, indent=0):
    string = "-" * indent
    for atom in parent.get_atoms():
        string += atom.type + " "
    print string
    for atom in parent.get_atoms():
        print atom.type
        print_flat(atom, indent=indent+1)

# Main function 
def createMp4(source, destination, start):
    mp4 = StreamMp4(source, destination, start)
    try:
        mp4.pushToStream()
    except Exception, e:
        raise
        print e

if __name__ == "__main__":
    args = ArgumentParser(formatter_class=RawDescriptionHelpFormatter,
                          description="Returns a new MP4")
    args.add_argument('source', help="Source to use", metavar="source")
    args.add_argument('dest', help="Destination to save at", metavar="dest")
    args.add_argument('start', help="Start duration", metavar="start", type=float)
    sargs = args.parse_args()    
    createMp4(sargs.source, sargs.dest, sargs.start)
    sys.exit()
