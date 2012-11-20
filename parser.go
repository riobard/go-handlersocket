package handlersocket

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

// write an encoded string token
func putBytes(w *bufio.Writer, b []byte) (err error) {
	for _, c := range b {
		if c > 0x0F {
			err = w.WriteByte(c)
			if err != nil {
				return
			}
		} else {
			err = w.WriteByte(0x01)
			if err != nil {
				return
			}
			err = w.WriteByte(c + 0x40)
			if err != nil {
				return
			}
		}
	}
	return
}

// Write a single value
func put(w *bufio.Writer, t interface{}) (err error) {
	switch v := t.(type) {
	case nil:
		err = w.WriteByte(0x00)
	case []byte:
		err = putBytes(w, v)
	case string:
		err = putBytes(w, []byte(v))
	case int:
		err = putBytes(w, []byte(strconv.Itoa(v)))
	default:
		err = errors.New("unexpecte type")
	}
	return
}

// Write multiple encoded string tokens separated by Tab (0x09).
// The end byte can be either Tab (0x09) or LF (0x0A) to append at the end.
// Any other values will be ignored. 
func mput(w *bufio.Writer, end byte, args ...interface{}) (err error) {
	if len(args) == 0 {
		return
	}
	for i := 0; i < len(args)-1; i++ {
		err = put(w, args[i])
		if err != nil {
			return
		}
		err = w.WriteByte(0x09)
		if err != nil {
			return
		}
	}

	err = put(w, args[len(args)-1])
	if err != nil {
		return
	}

	switch end {
	case 0x09:
		err = w.WriteByte(0x09)
	case 0x0A:
		err = w.WriteByte(0x0A)
	}
	return
}

// FSM-based parser of HandlerSocket protocol
func parse(r *bufio.Reader) (line []string, err error) {
	var c byte
	var buf bytes.Buffer
	state := 0
	for {
		if c, err = r.ReadByte(); err != nil {
			line = nil
			return
		}

		switch state {
		case 0:
			switch {
			case c == 0x00:
				state = 3
			case c == 0x01:
				state = 2
			case c == 0x09:
				line = append(line, "")
			case 0x10 <= c && c <= 0xFF:
				buf.WriteByte(c)
				state = 1
			default:
				line = nil
				err = errors.New(fmt.Sprintf("illegal byte %d in state %d", c, state))
				return
			}
		case 1:
			switch {
			case c == 0x01:
				state = 2
			case c == 0x09:
				line = append(line, buf.String())
				buf.Reset()
				state = 0
			case c == 0x0A:
				line = append(line, buf.String())
				return
			case 0x10 <= c && c <= 0xFF:
				buf.WriteByte(c)
			default:
				line = nil
				err = errors.New(fmt.Sprintf("illegal byte %d in state %d", c, state))
				return
			}
		case 2:
			switch {
			case 0x40 <= c && c <= 0x4F:
				buf.WriteByte(c - 0x40)
				state = 1
			default:
				line = nil
				err = errors.New(fmt.Sprintf("illegal byte %d in state %d", c, state))
				return
			}
		case 3:
			switch {
			case c == 0x09:
				line = append(line, "\x00")
				state = 0
			case c == 0x0A:
				line = append(line, "\x00")
				return
			default:
				line = nil
				err = errors.New(fmt.Sprintf("illegal byte %d in state %d", c, state))
				return
			}
		default:
			line = nil
			err = errors.New(fmt.Sprintf("illegal state %d", state))
			return
		}
	}
	line = nil
	err = errors.New("should never reach here")
	return
}

// check response and return errors if necessary
func check(line []string) (rsp []string, err error) {
	if len(line) < 2 {
		err = errors.New("response of insufficent length")
		return
	}

	if line[0] != "0" {
		if len(line) > 2 {
			err = errors.New(fmt.Sprintf("HandlerSocket error %s: %s", line[0], line[2]))
		} else {
			err = errors.New(fmt.Sprintf("HandlerSocket error %s", line[0]))
		}
		return
	}
	rsp = line[2:]
	return
}
