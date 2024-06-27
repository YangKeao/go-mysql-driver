package mysql

import (
	"database/sql/driver"
	"io"
)

type cursorRows struct {
	mc          *mysqlConn
	columns     []mysqlField
	lastRowSent bool

	buffer [][]driver.Value
	ptr    int

	fetchSize uint32
	id        uint32

	finish func()
}

func (rows *cursorRows) SetFinish(finish func()) {
	rows.finish = finish
}

func (rows *cursorRows) Columns() []string {
	// TODO: implement `Columns`. Refer to `mysqlRows.Columns` in rows.go
	return nil
}

func (rows *cursorRows) Close() error {
	if rows.finish != nil {
		rows.finish()
		rows.finish = nil
	}
	if rows.mc == nil || rows.mc.closed.Load() {
		// driver.Stmt.Close can be called more than once, thus this function
		// has to be idempotent.
		// See also Issue #450 and golang/go#16019.
		//errLog.Print(ErrInvalidConn)
		return driver.ErrBadConn
	}

	// only close the cursor, but not close the statement
	err := rows.mc.writeCommandPacketUint32(comStmtReset, rows.id)
	rows.mc = nil
	return err
}

func (rows *cursorRows) Next(dest []driver.Value) error {
	if rows.ptr >= len(rows.buffer) && !rows.lastRowSent {
		err := rows.fetchAndReadRows()
		if err != nil {
			return err
		}
		if rows.mc.status&statusLastRowSent > 0 {
			rows.lastRowSent = true
		}

		rows.ptr = 0
	}
	if rows.ptr >= len(rows.buffer) {
		return io.EOF
	}

	copy(dest, rows.buffer[rows.ptr])
	rows.ptr++
	return nil
}

func (rows *cursorRows) fetchAndReadRows() error {
	err := rows.writeFetchPacket(rows.fetchSize)
	if err != nil {
		return rows.mc.markBadConn(err)
	}

	internalRows := new(binaryRows)
	internalRows.mc = rows.mc
	internalRows.rs.columns = rows.columns

	// store all columns in the rows object
	rows.buffer = make([][]driver.Value, 0, rows.fetchSize)
loop:
	for {
		nextRow := make([]driver.Value, len(internalRows.rs.columns))
		err := internalRows.Next(nextRow)
		if err != nil {
			switch err {
			case io.EOF:
				break loop
			default:
				return err
			}
		}
		rows.buffer = append(rows.buffer, nextRow)
	}
	return nil
}

func (rows *cursorRows) writeFetchPacket(fetchSize uint32) error {
	// Reset packet-sequence
	rows.mc.sequence = 0

	// FETCH command: 1 byte status + 4 byte statement id + 4 byte num rows
	pktLen := 4 + 1 + 4 + 4
	data, err := rows.mc.buf.takeSmallBuffer(pktLen)
	if err != nil {
		// cannot take the buffer. Something must be wrong with the connection
		rows.mc.log(err)
		return errBadConnNoWrite
	}

	// command [1 byte]
	data[4] = comStmtFetch

	// statement_id [4 bytes]
	data[5] = byte(rows.id)
	data[6] = byte(rows.id >> 8)
	data[7] = byte(rows.id >> 16)
	data[8] = byte(rows.id >> 24)

	// num_rows [4 bytes]
	data[9] = byte(rows.fetchSize)
	data[10] = byte(rows.fetchSize >> 8)
	data[11] = byte(rows.fetchSize >> 16)
	data[12] = byte(rows.fetchSize >> 24)
	return rows.mc.writePacket(data)
}
