# raftlog.py
#
# This file implements the internal Raft log as a Python object.
# It is a stand-alone class that operates independently of any
# other part to make it easy to test and reason about.

# All entries in the Raft log have a term number and a value

from typing import NamedTuple, Any

class Entry(NamedTuple):
    term: int
    value: Any
    
class RaftLog:
    def __init__(self):
        self.entries = [ ]

    def __len__(self):
        return len(self.entries)

    def __getitem__(self, n):
        return self.entries[n]
    
    def append_entries(self, prev_index, prev_term, entries):
        # prev_index is the index of the log item immediately preceding the new entries.
        # prev_term is the expected term of the log item immediately preceding new entries.
        assert all(isinstance(e, Entry) for e in entries)
        
        # No holes/gaps are allowed in the log.
        if prev_index >= len(self.entries):
            return False

        # If the log is empty, it always succeeds
        if prev_index < 0:
            self.entries[:] = entries
            return True

        # If the term of previous entry doesn't match the prev_term provided, it fails.
        if self.entries[prev_index].term != prev_term:
            return False

        # If there are existing entries with a different term, they need to be deleted
        # See note 3 in figure 2.

        if entries and \
           prev_index + 1 < len(self.entries) and \
           self.entries[prev_index+1].term != entries[0].term:
            del self.entries[prev_index+1:]
 
        self.entries[prev_index+1:prev_index+1+len(entries)] = entries

        # Success!  Returning True indicates the append worked.
        return True

def test_log():
    log = RaftLog()
    assert len(log) == 0

    # Add a first entry to the log
    assert log.append_entries(-1,-1, [ Entry(0, 123) ]) == True
    assert len(log) == 1

    # Try adding an entry out of range.  Should fail (no holes)
    assert log.append_entries(10, 0, [ Entry(0, 42) ]) == False

    # Try adding an entry where previous term number doesn't match. Should fail.
    assert log.append_entries(0, 1, [ Entry(1, 42) ]) == False

    # Try adding an entry where the previous term does match. Should work.
    assert log.append_entries(0, 0, [ Entry(1, 42) ]) == True
    assert len(log) == 2
    assert log[1] == Entry(1, 42)

    # Try replacing an existing entry.  This can happen if a leader crashed and
    # had uncommitted entries in the log.   The uncommitted entries might be overwritten
    # by the new leader.
    assert log.append_entries(0, 0, [ Entry(2, 61) ]) == True
    assert len(log) == 2
    assert log[1] == Entry(2, 61)
    
    # If a log entry is replaced and there are entries following the new ones and
    # the log entry has a different term. They should be deleted.
    assert log.append_entries(-1, -1, [ Entry(3, 23) ]) == True
    assert len(log) == 1, log.entries
    
test_log()

    
