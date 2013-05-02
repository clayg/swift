"""
@project MP4 Stream
@author Young Kim (ykim@crunchyroll.com)

StreamAtoms.py - Outlines generic Atom structure to be used for parsing
                 ISO 14996-12 compliant MP4s
"""
import os
import struct

from Helper import read8, read24, read32, read64, type_to_str, EndOfFile
from StreamExceptions import *

# ISO 14996-12 Atoms that are Trees
ATOM_TREES = [ 'moov', 'trak', 'edts', 'mdia',
               'minf', 'dinf', 'stbl', 'mvex',
               'moof', 'traf', 'mfra', 'skip',
               'udta', 'meta', 'dinf', 'ipro',
               'sinf', 'fiin', 'paen', 'meco'
               ]

# Parses an Atom Tree
def parse_atom_tree(mp4, range, start):
    jump_to_pos = None
    atoms = []
    while mp4.tell() < range:
        atom = parse_atom(mp4, start)
        atoms.append(atom)
        print atom.offset, atom.size, atom.__class__.__name__
        if mp4.tell() != atom.offset + atom.size:
            # we have to fix the screwed up pos from the underlying call...
            bytes_to_consume = atom.offset + atom.size - mp4.tell()
            if bytes_to_consume < 0:
                print 'SHIT going back %s bytes' % bytes_to_consume
                mp4.seek(atom.offset + atom.size, os.SEEK_SET)
            else:
                if bytes_to_consume > 4 * 2 ** 20:
                    jump_to_pos = atom.offset + atom.size
                    print 'found jump_to_pos: %s' % jump_to_pos 
                    break
                print 'consuming %s bytes' % bytes_to_consume
                print bytes_to_consume == atom.size
                mp4.read(bytes_to_consume)
    return atoms, jump_to_pos

# Parses an Atom
def parse_atom(mp4, start):
    try:
        offset = mp4.tell()
        is_64 = False
        size = read32(mp4)
        type = type_to_str(read32(mp4))
        if (size == 1):
            size = read64(mp4)
            is_64 = True
        elif (size == 0):
            if hasattr(mp4, 'fileno'):
                size = (os.fstat(mp4.fileno()).st_size - offset)
            else:
                size = (mp4.len - offset)
        if offset + start > 4 * 2 ** 20:
            print 'offset: %s' % offset
            print 'start: %s' % start
            raise('FOUND IT!')
        return create_atom(mp4, offset, size, type, is_64, start)
    except EndOfFile:
        raise
        return None

def create_atom(mp4, offset, size, type, is_64, start):
    try:
        return eval("%s(mp4, offset, size, type, is_64, start)" % type)
    except NameError:
        if type in ATOM_TREES:
            return StreamAtomTree(mp4, offset, size, type, is_64, start)
        else:
            return StreamAtom(mp4, offset, size, type, is_64, start)
    except TypeError:
        return StreamAtom(mp4, offset, size, type, is_64, start)

# Generic StreamAtom Object - Equivalent to Box in ISO specs
class StreamAtom(object):
    # Copy verifies if parsed Atom should be copied into Stream
    copy = False
    
    def __init__(self, file, offset, size, type, is_64, start):
        self.file = file
        self.offset = offset
        self.size = size
        self.type = type
        self.is_64 = is_64
        self.start = start
        self.children = []
        self.attrs = {}
    
    def _set_attr(self, key, value):
        self.attrs[key] = value
    
    def _set_children(self, children):
        for child in children:
            child.parent = self
        self.children = children
    
    def get_attribute(self, key):
        return self.attrs[key]
    
    def get_atoms(self):
        return self.children
    
    # Prepare StreamAtom to be pushed into a stream
    def update(self):
        raise NotImplementedError()
    
    # Returns amount of bytes copied into file
    def pushToStream(self, stream):
        raise NotImplementedError()
    

# Generic StreamFullAtom - Equivalent to FullBox in ISO specs
class StreamFullAtom(StreamAtom):
    def  __init__(self, file, offset, size, type, is_64, start):
        StreamAtom.__init__(self, file, offset, size, type, is_64, start)
        self.version = read8(file)
        self.bit = read24(file)
    

# Generic StreamAtomTree - Represents a Tree of Atoms
class StreamAtomTree(StreamAtom):
    def __init__(self, file, offset, size, type, is_64, start):
        StreamAtom.__init__(self, file, offset, size, type, is_64, start)
        print 'PARSE ATOM TREE'
        children, jump_to_pos = parse_atom_tree(file, offset+size, start)
        self.jump_to_pos = jump_to_pos
        self._set_children(children)
        self.update_order = []
        self.stream_order = []
    
    def update(self):
        if self.copy:
            # Force each atom that is copyable to update itself
            if self.update_order:
                for type in self.update_order:
                    for atom in self.get_atoms():
                        if atom.copy and atom.type == type:
                            atom.update()
            else:
                for atom in self.get_atoms():
                    if atom.copy:
                        atom.update()
            atom_size = 0
            for atom in self.get_atoms():
                if atom.copy:
                    atom_size += atom.size
            # Calculate if 64 bit flag has to be written in
            if atom_size > 4294967287:
                self.size = atom_size + 16
                self.is_64 = True
            else:
                self.size = atom_size + 8
                self.is_64 = False
    
    def pushToStream(self, stream):
        if self.copy:
            if self.is_64:
                stream.write(struct.pack(">I4sQ", 1, self.type, self.size))
            else:
                stream.write(struct.pack(">I4s", self.size, self.type))
            if self.stream_order:
                for type in self.stream_order:
                    for atom in self.get_atoms():
                        if atom.copy and atom.type == type:
                            atom.pushToStream(stream)
            else:
                for atom in self.get_atoms():
                    if atom.copy:
                        atom.pushToStream(stream)
    

# Import specific Mp4Atoms
from StreamMp4Atoms import *
