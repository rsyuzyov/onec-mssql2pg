package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

// encoder пишет значение колонки в формате COPY text.
type encoder func(w *bufio.Writer, v any) error

// Статистика необычных значений, которые пришлось поправить.
type fixes struct {
	nulChars int64 // символы U+0000 в строках: PG их не хранит, удаляются
}

func writeNull(w *bufio.Writer) { w.WriteString(`\N`) }

func writeText(w *bufio.Writer, s string, fx *fixes) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\\':
			w.WriteString(`\\`)
		case '\n':
			w.WriteString(`\n`)
		case '\r':
			w.WriteString(`\r`)
		case '\t':
			w.WriteString(`\t`)
		case 0:
			fx.nulChars++
		default:
			w.WriteByte(c)
		}
	}
}

func writeBytea(w *bufio.Writer, b []byte) {
	w.WriteString(`\\x`)
	var tmp [64]byte
	for len(b) > 0 {
		n := len(b)
		if n > 32 {
			n = 32
		}
		hex.Encode(tmp[:n*2], b[:n])
		w.Write(tmp[:n*2])
		b = b[n:]
	}
}

func makeEncoder(c Col, yearOffset int, fx *fixes) (encoder, error) {
	if c.MsType == "timestamp" {
		// rowversion MS SQL (_Version) в PostgreSQL — счетчик int4; платформа при переносе
		// базы (ibcmd infobase replicate) начинает его с 1, так же делаем и мы.
		return func(w *bufio.Writer, v any) error {
			if v == nil {
				writeNull(w)
			} else {
				w.WriteByte('1')
			}
			return nil
		}, nil
	}
	switch c.PgType {
	case "bytea":
		return func(w *bufio.Writer, v any) error {
			switch x := v.(type) {
			case nil:
				writeNull(w)
			case []byte:
				writeBytea(w, x)
			default:
				return fmt.Errorf("bytea: неожиданный тип %T", v)
			}
			return nil
		}, nil
	case "bool":
		return func(w *bufio.Writer, v any) error {
			switch x := v.(type) {
			case nil:
				writeNull(w)
			case bool:
				if x {
					w.WriteByte('t')
				} else {
					w.WriteByte('f')
				}
			case []byte:
				t := false
				for _, b := range x {
					if b != 0 {
						t = true
						break
					}
				}
				if t {
					w.WriteByte('t')
				} else {
					w.WriteByte('f')
				}
			case int64:
				if x != 0 {
					w.WriteByte('t')
				} else {
					w.WriteByte('f')
				}
			default:
				return fmt.Errorf("bool: неожиданный тип %T", v)
			}
			return nil
		}, nil
	case "timestamp", "timestamptz", "date":
		off := yearOffset
		return func(w *bufio.Writer, v any) error {
			switch x := v.(type) {
			case nil:
				writeNull(w)
			case time.Time:
				if off != 0 {
					x = x.AddDate(-off, 0, 0)
				}
				var tmp [40]byte
				w.Write(x.AppendFormat(tmp[:0], "2006-01-02 15:04:05.999999"))
			default:
				return fmt.Errorf("timestamp: неожиданный тип %T", v)
			}
			return nil
		}, nil
	case "numeric", "int2", "int4", "int8", "float4", "float8":
		return func(w *bufio.Writer, v any) error {
			var tmp [32]byte
			switch x := v.(type) {
			case nil:
				writeNull(w)
			case []byte:
				w.Write(x)
			case string:
				w.WriteString(x)
			case int64:
				w.Write(strconv.AppendInt(tmp[:0], x, 10))
			case float64:
				w.Write(strconv.AppendFloat(tmp[:0], x, 'g', -1, 64))
			case bool:
				if x {
					w.WriteByte('1')
				} else {
					w.WriteByte('0')
				}
			default:
				return fmt.Errorf("число: неожиданный тип %T", v)
			}
			return nil
		}, nil
	default: // mvarchar, mchar, text, varchar, bpchar
		return func(w *bufio.Writer, v any) error {
			switch x := v.(type) {
			case nil:
				writeNull(w)
			case string:
				writeText(w, x, fx)
			case []byte:
				writeText(w, string(x), fx)
			case int64:
				var tmp [32]byte
				w.Write(strconv.AppendInt(tmp[:0], x, 10))
			default:
				return fmt.Errorf("строка: неожиданный тип %T", v)
			}
			return nil
		}, nil
	}
}
