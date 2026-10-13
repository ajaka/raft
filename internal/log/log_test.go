package log

import (
	"os"
	"testing"

	rpc "raft/internal/rpc/gen"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogInit(t *testing.T) {
	tmpFile := "test_raft_init.log"
	defer os.Remove(tmpFile)

	l := Log{
		index:        make(map[uint64]uint64),
		filename:     tmpFile,
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
	err := l.Init()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), l.GetLastIndex())
	assert.Equal(t, uint64(0), l.GetLastTerm())
	assert.NotNil(t, l.file)
}

const testEntryType = rpc.EntryType(1) // SET

func TestBuildIndexEmptyLog(t *testing.T) {
	tmpFile := "test_raft_empty.log"
	defer os.Remove(tmpFile)

	l := Log{
		index:        make(map[uint64]uint64),
		filename:     tmpFile,
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
	err := l.Init()
	require.NoError(t, err)

	// Empty log: index[1] = 0 (phantom), lastLogIndex = 0
	_, ok := l.index[1]
	assert.True(t, ok)
	assert.Equal(t, uint64(0), l.index[1])

	assert.Equal(t, uint64(0), l.GetLastIndex())
	assert.Equal(t, uint64(0), l.GetLastTerm())

	// TermAt(1) should fail on empty log (index 1 is phantom)
	_, ok = l.TermAt(1)
	assert.False(t, ok)

	// GetEntry(1) should fail on empty log
	_, ok = l.GetEntry(1)
	assert.False(t, ok)
}

func TestAppendSingleEntry(t *testing.T) {
	tmpFile := "test_raft_single.log"
	defer os.Remove(tmpFile)

	l := Log{
		index:        make(map[uint64]uint64),
		filename:     tmpFile,
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
	err := l.Init()
	require.NoError(t, err)

	entry := Entry{
		Index:   1,
		Term:    1,
		Type:    testEntryType,
		Payload: []byte("key=value"),
	}
	err = l.AppendEntries([]Entry{entry})
	require.NoError(t, err)

	assert.Equal(t, uint64(1), l.GetLastIndex())
	assert.Equal(t, uint64(1), l.GetLastTerm())

	// Verify TermAt
	term, ok := l.TermAt(1)
	assert.True(t, ok)
	assert.Equal(t, uint64(1), term)

	// Verify GetEntry
	got, ok := l.GetEntry(1)
	assert.True(t, ok)
	assert.Equal(t, entry.Index, got.Index)
	assert.Equal(t, entry.Term, got.Term)
	assert.Equal(t, entry.Type, got.Type)
	assert.Equal(t, entry.Payload, got.Payload)

	// Verify index map has correct entries
	assert.Equal(t, uint64(0), l.index[1]) // start of entry 1
	_, ok = l.index[2]
	assert.True(t, ok) // start of entry 2 (EOF)
}

func TestAppendMultipleEntries(t *testing.T) {
	tmpFile := "test_raft_multi.log"
	defer os.Remove(tmpFile)

	l := Log{
		index:        make(map[uint64]uint64),
		filename:     tmpFile,
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
	err := l.Init()
	require.NoError(t, err)

	entries := []Entry{
		{Index: 1, Term: 1, Type: testEntryType, Payload: []byte("a=1")},
		{Index: 2, Term: 1, Type: testEntryType, Payload: []byte("b=2")},
		{Index: 3, Term: 2, Type: testEntryType, Payload: []byte("c=3")},
	}
	err = l.AppendEntries(entries)
	require.NoError(t, err)

	assert.Equal(t, uint64(3), l.GetLastIndex())
	assert.Equal(t, uint64(2), l.GetLastTerm())

	// Verify all entries
	for i, e := range entries {
		got, ok := l.GetEntry(uint64(i + 1))
		assert.True(t, ok, "entry %d should exist", i+1)
		assert.Equal(t, e.Index, got.Index)
		assert.Equal(t, e.Term, got.Term)
		assert.Equal(t, e.Type, got.Type)
		assert.Equal(t, e.Payload, got.Payload)
	}

	// Verify TermAt for each index
	assert.Equal(t, uint64(1), func() uint64 { t, _ := l.TermAt(1); return t }())
	assert.Equal(t, uint64(1), func() uint64 { t, _ := l.TermAt(2); return t }())
	assert.Equal(t, uint64(2), func() uint64 { t, _ := l.TermAt(3); return t }())
}

func TestGetEntries(t *testing.T) {
	tmpFile := "test_raft_getentries.log"
	defer os.Remove(tmpFile)

	l := Log{
		index:        make(map[uint64]uint64),
		filename:     tmpFile,
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
	err := l.Init()
	require.NoError(t, err)

	entries := []Entry{
		{Index: 1, Term: 1, Type: testEntryType, Payload: []byte("a=1")},
		{Index: 2, Term: 1, Type: testEntryType, Payload: []byte("b=2")},
		{Index: 3, Term: 2, Type: testEntryType, Payload: []byte("c=3")},
	}
	err = l.AppendEntries(entries)
	require.NoError(t, err)

	// Get all entries from index 1
	got := l.GetEntries(1)
	assert.Len(t, got, 3)
	for i, e := range entries {
		assert.Equal(t, e.Index, got[i].Index)
		assert.Equal(t, e.Term, got[i].Term)
	}

	// Get from middle
	got = l.GetEntries(2)
	assert.Len(t, got, 2)
	assert.Equal(t, uint64(2), got[0].Index)
	assert.Equal(t, uint64(3), got[1].Index)

	// Get from past end
	got = l.GetEntries(5)
	assert.Len(t, got, 0)
}

func TestTruncate(t *testing.T) {
	tmpFile := "test_raft_truncate.log"
	defer os.Remove(tmpFile)

	l := Log{
		index:        make(map[uint64]uint64),
		filename:     tmpFile,
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
	err := l.Init()
	require.NoError(t, err)

	entries := []Entry{
		{Index: 1, Term: 1, Type: testEntryType, Payload: []byte("a=1")},
		{Index: 2, Term: 1, Type: testEntryType, Payload: []byte("b=2")},
		{Index: 3, Term: 2, Type: testEntryType, Payload: []byte("c=3")},
		{Index: 4, Term: 2, Type: testEntryType, Payload: []byte("d=4")},
	}
	err = l.AppendEntries(entries)
	require.NoError(t, err)

	// Truncate at index 3 (keep 1,2; remove 3,4)
	l.truncate(3)

	assert.Equal(t, uint64(2), l.GetLastIndex())
	assert.Equal(t, uint64(1), l.GetLastTerm())

	// Entries 1,2 should still exist
	_, ok := l.GetEntry(1)
	assert.True(t, ok)
	_, ok = l.GetEntry(2)
	assert.True(t, ok)

	// Entries 3,4 should be gone
	_, ok = l.GetEntry(3)
	assert.False(t, ok)
	_, ok = l.GetEntry(4)
	assert.False(t, ok)

	// Index map should be rebuilt: index[3] exists but points to EOF
	assert.Equal(t, uint64(0), l.index[1])
	_, ok = l.index[2]
	assert.True(t, ok)
	_, ok = l.index[3]
	assert.True(t, ok) // exists but points to EOF
}

func TestTruncateAtZero(t *testing.T) {
	tmpFile := "test_raft_truncate_zero.log"
	defer os.Remove(tmpFile)

	l := Log{
		index:        make(map[uint64]uint64),
		filename:     tmpFile,
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
	err := l.Init()
	require.NoError(t, err)

	entries := []Entry{
		{Index: 1, Term: 1, Type: testEntryType, Payload: []byte("a=1")},
		{Index: 2, Term: 1, Type: testEntryType, Payload: []byte("b=2")},
	}
	err = l.AppendEntries(entries)
	require.NoError(t, err)

	l.truncate(0)

	assert.Equal(t, uint64(0), l.GetLastIndex())
	assert.Equal(t, uint64(0), l.GetLastTerm())
	_, ok := l.GetEntry(1)
	assert.False(t, ok)
}

func TestTruncateOutOfBounds(t *testing.T) {
	tmpFile := "test_raft_truncate_oob.log"
	defer os.Remove(tmpFile)

	l := Log{
		index:        make(map[uint64]uint64),
		filename:     tmpFile,
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
	err := l.Init()
	require.NoError(t, err)

	entries := []Entry{
		{Index: 1, Term: 1, Type: testEntryType, Payload: []byte("a=1")},
	}
	err = l.AppendEntries(entries)
	require.NoError(t, err)

	// Truncate at index 5 (past end) - should be no-op
	l.truncate(5)

	assert.Equal(t, uint64(1), l.GetLastIndex())
	assert.Equal(t, uint64(1), l.GetLastTerm())
	_, ok := l.GetEntry(1)
	assert.True(t, ok)
}

func TestAppendWithConflictTruncates(t *testing.T) {
	tmpFile := "test_raft_conflict.log"
	defer os.Remove(tmpFile)

	l := Log{
		index:        make(map[uint64]uint64),
		filename:     tmpFile,
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
	err := l.Init()
	require.NoError(t, err)

	// Existing log: index 1 (term 1), index 2 (term 1), index 3 (term 2)
	entries := []Entry{
		{Index: 1, Term: 1, Type: testEntryType, Payload: []byte("a=1")},
		{Index: 2, Term: 1, Type: testEntryType, Payload: []byte("b=2")},
		{Index: 3, Term: 2, Type: testEntryType, Payload: []byte("c=3")},
	}
	err = l.AppendEntries(entries)
	require.NoError(t, err)

	// New entries conflict at index 2 (different term)
	newEntries := []Entry{
		{Index: 2, Term: 3, Type: testEntryType, Payload: []byte("b=new")},
		{Index: 3, Term: 3, Type: testEntryType, Payload: []byte("c=new")},
		{Index: 4, Term: 3, Type: testEntryType, Payload: []byte("d=4")},
	}
	err = l.AppendEntries(newEntries)
	require.NoError(t, err)

	// Should have truncated at index 2 and appended new entries
	assert.Equal(t, uint64(4), l.GetLastIndex())
	assert.Equal(t, uint64(3), l.GetLastTerm())

	// Verify new entries
	got, ok := l.GetEntry(1)
	assert.True(t, ok)
	assert.Equal(t, []byte("a=1"), got.Payload) // unchanged

	got, ok = l.GetEntry(2)
	assert.True(t, ok)
	assert.Equal(t, uint64(3), got.Term)
	assert.Equal(t, []byte("b=new"), got.Payload)

	got, ok = l.GetEntry(3)
	assert.True(t, ok)
	assert.Equal(t, uint64(3), got.Term)
	assert.Equal(t, []byte("c=new"), got.Payload)

	got, ok = l.GetEntry(4)
	assert.True(t, ok)
	assert.Equal(t, uint64(3), got.Term)
	assert.Equal(t, []byte("d=4"), got.Payload)
}

func TestAppendMatchingEntriesNoTruncate(t *testing.T) {
	tmpFile := "test_raft_match.log"
	defer os.Remove(tmpFile)

	l := Log{
		index:        make(map[uint64]uint64),
		filename:     tmpFile,
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
	err := l.Init()
	require.NoError(t, err)

	entries := []Entry{
		{Index: 1, Term: 1, Type: testEntryType, Payload: []byte("a=1")},
		{Index: 2, Term: 1, Type: testEntryType, Payload: []byte("b=2")},
		{Index: 3, Term: 2, Type: testEntryType, Payload: []byte("c=3")},
	}
	err = l.AppendEntries(entries)
	require.NoError(t, err)

	// Append matching entries starting at index 3
	newEntries := []Entry{
		{Index: 3, Term: 2, Type: testEntryType, Payload: []byte("c=3")}, // matches
		{Index: 4, Term: 2, Type: testEntryType, Payload: []byte("d=4")}, // new
	}
	err = l.AppendEntries(newEntries)
	require.NoError(t, err)

	assert.Equal(t, uint64(4), l.GetLastIndex())

	// Original entry 3 should be unchanged
	got, ok := l.GetEntry(3)
	assert.True(t, ok)
	assert.Equal(t, []byte("c=3"), got.Payload)
}

func TestPersistAfterRestart(t *testing.T) {
	tmpFile := "test_raft_persist.log"
	defer os.Remove(tmpFile)

	// First process: write entries
	{
		l := Log{
			index:        make(map[uint64]uint64),
			filename:     tmpFile,
			lastLogIndex: 0,
			lastLogTerm:  0,
		}
		err := l.Init()
		require.NoError(t, err)

		entries := []Entry{
			{Index: 1, Term: 1, Type: testEntryType, Payload: []byte("a=1")},
			{Index: 2, Term: 2, Type: testEntryType, Payload: []byte("b=2")},
		}
		err = l.AppendEntries(entries)
		require.NoError(t, err)

		l.file.Close()
	}

	// Second process: reopen and verify
	{
		l := Log{
			index:        make(map[uint64]uint64),
			filename:     tmpFile,
			lastLogIndex: 0,
			lastLogTerm:  0,
		}
		err := l.Init()
		require.NoError(t, err)

		assert.Equal(t, uint64(2), l.GetLastIndex())
		assert.Equal(t, uint64(2), l.GetLastTerm())

		got, ok := l.GetEntry(1)
		assert.True(t, ok)
		assert.Equal(t, []byte("a=1"), got.Payload)

		got, ok = l.GetEntry(2)
		assert.True(t, ok)
		assert.Equal(t, []byte("b=2"), got.Payload)
	}
}

func TestTermAtOutOfBounds(t *testing.T) {
	tmpFile := "test_raft_term_oob.log"
	defer os.Remove(tmpFile)

	l := Log{
		index:        make(map[uint64]uint64),
		filename:     tmpFile,
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
	err := l.Init()
	require.NoError(t, err)

	entries := []Entry{
		{Index: 1, Term: 1, Type: testEntryType, Payload: []byte("a=1")},
	}
	err = l.AppendEntries(entries)
	require.NoError(t, err)

	// Valid index
	term, ok := l.TermAt(1)
	assert.True(t, ok)
	assert.Equal(t, uint64(1), term)

	// Out of bounds
	_, ok = l.TermAt(2)
	assert.False(t, ok)

	_, ok = l.TermAt(0) // index 0 is not a valid entry index
	assert.False(t, ok)
}

func TestBuildIndexCorruptedTail(t *testing.T) {
	tmpFile := "test_raft_corrupt.log"
	defer os.Remove(tmpFile)

	l := Log{
		index:        make(map[uint64]uint64),
		filename:     tmpFile,
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
	err := l.Init()
	require.NoError(t, err)

	entries := []Entry{
		{Index: 1, Term: 1, Type: testEntryType, Payload: []byte("a=1")},
		{Index: 2, Term: 1, Type: testEntryType, Payload: []byte("b=2")},
	}
	err = l.AppendEntries(entries)
	require.NoError(t, err)

	l.file.Close()

	// Corrupt the file by truncating mid-entry
	f, err := os.OpenFile(tmpFile, os.O_RDWR, 0644)
	require.NoError(t, err)
	info, _ := f.Stat()
	// Truncate to middle of second entry
	f.Truncate(info.Size() / 2)
	f.Close()

	// Reopen - BuildIndex should handle truncated entry
	l2 := Log{
		index:        make(map[uint64]uint64),
		filename:     tmpFile,
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
	err = l2.Init()
	require.NoError(t, err)

	// Should have only first entry (second was corrupted and truncated)
	assert.Equal(t, uint64(1), l2.GetLastIndex())
	_, ok := l2.GetEntry(1)
	assert.True(t, ok)
	_, ok = l2.GetEntry(2)
	assert.False(t, ok)
}
