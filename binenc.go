package main

import (
	"bufio"
	"database/sql"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"
	"unicode/utf16"
	"unicode/utf8"
)

// Двоичный COPY: строки MS SQL (nvarchar, nchar) читаются как varbinary, то есть сразу в UTF-16LE,
// а mvarchar/mchar 1С в двоичном виде тоже UTF-16LE — перекодировок нет. Ссылки binary(16)
// уходят сырыми байтами, без шестнадцатеричного текста.

var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()

// binCol — чтение одной колонки: выражение в SELECT, приемник Scan и кодировщик.
type binCol struct {
	expr string
	dest any
	enc  func(w *bufio.Writer) error
}

func putLen(w *bufio.Writer, n int) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(int32(n)))
	w.Write(b[:])
}

func putNull(w *bufio.Writer) { putLen(w, -1) }

// writeUTF16 пишет UTF-16LE, выбрасывая символы U+0000.
func writeUTF16(w *bufio.Writer, b []byte, fx *fixes) {
	nul := 0
	for i := 0; i+1 < len(b); i += 2 {
		if b[i] == 0 && b[i+1] == 0 {
			nul++
		}
	}
	if nul == 0 {
		putLen(w, len(b))
		w.Write(b)
		return
	}
	fx.nulChars += int64(nul)
	putLen(w, len(b)-2*nul)
	for i := 0; i+1 < len(b); i += 2 {
		if b[i] != 0 || b[i+1] != 0 {
			w.WriteByte(b[i])
			w.WriteByte(b[i+1])
		}
	}
}

// utf16to8 — для редких колонок PG типа text/varchar.
func utf16to8(b []byte) string {
	u := make([]uint16, len(b)/2)
	for i := range u {
		u[i] = binary.LittleEndian.Uint16(b[2*i:])
	}
	return string(utf16.Decode(u))
}

// numericBin — двоичное представление numeric PostgreSQL из десятичной строки "−123.4500".
func numericBin(s []byte, buf []byte) ([]byte, error) {
	neg := false
	if len(s) > 0 && (s[0] == '-' || s[0] == '+') {
		neg = s[0] == '-'
		s = s[1:]
	}
	intPart, frac := s, []byte(nil)
	for i, c := range s {
		if c == '.' {
			intPart, frac = s[:i], s[i+1:]
			break
		}
	}
	for _, c := range intPart {
		if c < '0' || c > '9' {
			return nil, fmt.Errorf("numeric: %q", s)
		}
	}
	for _, c := range frac {
		if c < '0' || c > '9' {
			return nil, fmt.Errorf("numeric: %q", s)
		}
	}
	dscale := len(frac)
	// Группы по 4 цифры вокруг десятичной точки.
	var digits []int16
	lead := (4 - len(intPart)%4) % 4
	nInt := (len(intPart) + lead) / 4
	grp := 0
	cnt := lead
	for _, c := range intPart {
		grp = grp*10 + int(c-'0')
		cnt++
		if cnt == 4 {
			digits = append(digits, int16(grp))
			grp, cnt = 0, 0
		}
	}
	grp, cnt = 0, 0
	for _, c := range frac {
		grp = grp*10 + int(c-'0')
		cnt++
		if cnt == 4 {
			digits = append(digits, int16(grp))
			grp, cnt = 0, 0
		}
	}
	if cnt > 0 {
		for ; cnt < 4; cnt++ {
			grp *= 10
		}
		digits = append(digits, int16(grp))
	}
	weight := nInt - 1
	for len(digits) > 0 && digits[0] == 0 {
		digits = digits[1:]
		weight--
	}
	for len(digits) > 0 && digits[len(digits)-1] == 0 {
		digits = digits[:len(digits)-1]
	}
	sign := uint16(0)
	if neg && len(digits) > 0 {
		sign = 0x4000
	}
	if len(digits) == 0 {
		weight = 0
	}
	buf = buf[:0]
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(digits)))
	buf = binary.BigEndian.AppendUint16(buf, uint16(int16(weight)))
	buf = binary.BigEndian.AppendUint16(buf, sign)
	buf = binary.BigEndian.AppendUint16(buf, uint16(dscale))
	for _, d := range digits {
		buf = binary.BigEndian.AppendUint16(buf, uint16(d))
	}
	return buf, nil
}

func isMsString(t string) bool {
	return t == "nvarchar" || t == "nchar" || t == "ntext" || t == "varchar" || t == "char" || t == "text"
}

// makeBinCol строит чтение и двоичный кодировщик колонки.
func makeBinCol(c Col, yearOffset int, fx *fixes) (*binCol, error) {
	col := qms(c.MsName)
	bc := &binCol{expr: col}
	switch {
	case c.MsType == "timestamp": // rowversion -> _version int4, как у платформы — 1
		var d sql.RawBytes
		bc.dest = &d
		bc.enc = func(w *bufio.Writer) error {
			if d == nil {
				putNull(w)
				return nil
			}
			putLen(w, 4)
			w.Write([]byte{0, 0, 0, 1})
			return nil
		}
	case isMsString(c.MsType):
		unicode := c.MsType[0] == 'n'
		if unicode {
			n := "max"
			if c.MsLen > 0 {
				n = strconv.Itoa(c.MsLen)
			}
			bc.expr = fmt.Sprintf("cast(%s as varbinary(%s))", col, n)
		}
		var d sql.RawBytes
		bc.dest = &d
		toText := c.PgType == "text" || c.PgType == "varchar" || c.PgType == "bpchar"
		bc.enc = func(w *bufio.Writer) error {
			if d == nil {
				putNull(w)
				return nil
			}
			switch {
			case unicode && !toText:
				writeUTF16(w, d, fx)
			case unicode && toText:
				s := utf16to8(d)
				putLen(w, len(s))
				w.WriteString(s)
			case !unicode && !toText: // однобайтовая строка в mvarchar — через UTF-16
				u := utf16.Encode([]rune(string(d)))
				putLen(w, len(u)*2)
				for _, x := range u {
					w.WriteByte(byte(x))
					w.WriteByte(byte(x >> 8))
				}
			default:
				if !utf8.Valid(d) {
					return fmt.Errorf("строка не UTF-8")
				}
				putLen(w, len(d))
				w.Write(d)
			}
			return nil
		}
	case c.PgType == "bool":
		if c.MsType == "bit" {
			var d sql.NullBool
			bc.dest = &d
			bc.enc = func(w *bufio.Writer) error {
				if !d.Valid {
					putNull(w)
					return nil
				}
				putLen(w, 1)
				if d.Bool {
					w.WriteByte(1)
				} else {
					w.WriteByte(0)
				}
				return nil
			}
		} else {
			var d sql.RawBytes
			bc.dest = &d
			bc.enc = func(w *bufio.Writer) error {
				if d == nil {
					putNull(w)
					return nil
				}
				v := byte(0)
				for _, b := range d {
					if b != 0 {
						v = 1
						break
					}
				}
				putLen(w, 1)
				w.WriteByte(v)
				return nil
			}
		}
	case c.PgType == "bytea":
		var d sql.RawBytes
		bc.dest = &d
		bc.enc = func(w *bufio.Writer) error {
			if d == nil {
				putNull(w)
				return nil
			}
			putLen(w, len(d))
			w.Write(d)
			return nil
		}
	case c.PgType == "timestamp":
		var d sql.NullTime
		bc.dest = &d
		off := yearOffset
		bc.enc = func(w *bufio.Writer) error {
			if !d.Valid {
				putNull(w)
				return nil
			}
			t := d.Time
			if off != 0 {
				t = t.AddDate(-off, 0, 0)
			}
			// Время без часового пояса: берем показания часов как UTC.
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(t.UnixMicro()-pgEpoch))
			putLen(w, 8)
			w.Write(b[:])
			return nil
		}
	case c.PgType == "numeric":
		var d sql.RawBytes
		bc.dest = &d
		var buf []byte
		bc.enc = func(w *bufio.Writer) error {
			if d == nil {
				putNull(w)
				return nil
			}
			var err error
			buf, err = numericBin(d, buf)
			if err != nil {
				return err
			}
			putLen(w, len(buf))
			w.Write(buf)
			return nil
		}
	case c.PgType == "int2" || c.PgType == "int4" || c.PgType == "int8":
		var d sql.NullInt64
		bc.dest = &d
		size := map[string]int{"int2": 2, "int4": 4, "int8": 8}[c.PgType]
		bc.enc = func(w *bufio.Writer) error {
			if !d.Valid {
				putNull(w)
				return nil
			}
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(d.Int64))
			putLen(w, size)
			w.Write(b[8-size:])
			return nil
		}
	default:
		return nil, fmt.Errorf("двоичный COPY: нет правила %s -> %s", c.MsType, c.PgType)
	}
	return bc, nil
}
