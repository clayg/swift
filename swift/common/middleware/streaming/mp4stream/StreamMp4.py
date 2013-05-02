"""
@project MP4 Stream
@author Young Kim (ykim@crunchyroll.com)

StreamMp4.py - Represents a StreamMp4 that is a Pseudo-stream equivalent
"""

import os
from StreamAtoms import StreamAtomTree

class StreamMp4(object):
    atoms = None
    def __init__(self, source, destination, start):
        self.source = source
        self.source_file = open(self.source, "rb")
        self.destination = destination
        self.start = int(start) * 1000
    
    # pushToStream - Converts source file for pseudo-streaming
    def pushToStream(self):
        # Parse the MP4 into StreamAtom elements
        print 'PARSE MP4'
        self._parseMp4()
        # Update StreamAtom elements
        print 'UPDATE ATOMS'
        self._updateAtoms()
        # Write to Stream
        print 'WRITE NEW'
        self._writeToStream()
    
    def _parseMp4(self):
        source_size = os.path.getsize(self.source)
        self.atoms = StreamAtomTree(self.source_file, 0, source_size, 
                                    '', False, self.start)
    
    def _updateAtoms(self):
        for atom in self.atoms.get_atoms():
            if atom.copy:
                atom.update()
    
    def _writeToStream(self):
        file = open(self.destination, "w")        
        for type in ["ftyp", "moov", "mdat"]:
            for atom in self.atoms.get_atoms():
                if atom.copy and atom.type == type:
                    atom.pushToStream(file)
        file.close()
    
    # getAtoms - Used primarily for debugging purposes
    def getAtoms(self):
        return self.atoms


class WriteableIterThing(object):

    def __init__(self):
        self.buf = []

    def write(self, bytes):
        print 'adding %s bytes to buffer' % len(bytes)
        self.buf.append(bytes)

    def __iter__(self):
        self.queue = iter(list(self.buf))
        self.buf = []
        return self

    def next(self):
        return self.queue.next()


class SwiftStreamMp4(StreamMp4):

    def __init__(self, source_file, source_size, start):
        self.source = None
        self.destination = None
        self.source_file = source_file
        self.source_size = source_size
        self.start = int(start) * 1000

    def _parseMp4(self):
        self.atoms = StreamAtomTree(self.source_file, 0, self.source_size, 
                                    '', False, self.start)

    def _yieldToStream(self):
        self.destination = WriteableIterThing()
        for type in ["ftyp", "moov", "mdat"]:
            for atom in self.atoms.get_atoms():
                if atom.copy and atom.type == type:
                    atom.pushToStream(self.destination)
                    for chunk in self.destination:
                        print 'yielding chunk'
                        yield chunk
