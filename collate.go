package collate

import (
	//"fmt"
	"github.com/guitarvydas/ip"
	"regexp"
	"strconv"
	"strings"
)

func Collate(name string, ctl <-chan string, in []chan ip.IP, out chan<- ip.IP) {
	fieldLengths, nFields := makeFieldArray(<-ctl)

	// Also, the record "keys" are characters in fields at the front of the record.  Calculate
	// the maximum key size so we can trim before comparing.
	// For example a master record might look like
	//
	// 111AA11111   M
	//
	// With 3 keys, the leftmost is 3 chars, then 2 chars then 5 chars
	// the data starts in the 11th column (we need to strip the data when
	// comparing keys).  In this example, keylen should be calculated to
	// be 3+2+5 = 10
	// Keys sort in ASCII order, e.g. "111" is less than "222", etc.
	//
	// Each key is nested by open/close brackets.  In the example above, there
	// are three fields in the key, so 3 open brackets are sent out initially.
	// When the key "level" changes, we close that level and send out a new open.
	// E.g. The above record, followed by a change to the inner-most field
	//
	// 111AA11111   M
	// 111AA22222   M
	//
	// would be sent as
	//
	// (((111AA1111 ... data ...)(111AA222 ... data ...)))
	//
	// i.e. the bracketing is done on a key field level, e.g. a key with
	// three fields will be at most 3 brackets deep
	keylen := 0
	for i := 0; i < nFields; i++ {
		out <- ip.IP{Kind: ip.Open}
		keylen += fieldLengths[i]
	}

	// nActive is used to detect when all input ports are EOF ; go recommends doing this
	// another way (using a second set of "done" channels), but we'll try this
	nActive := len(in)
	nPorts := len(in)

	// create a holding slot for every channel, and read first entry into each
	// set the initial value of lowestKey and lowestPort
	highestKey := makeKey(keylen, 255)
	lowestKey := highestKey // init high
	lowestKeyFields := sliceKey(lowestKey, fieldLengths)
	lowestPort := 0
	parray := make([]ip.IP, len(in))
	for i := 0; nActive > 0 && i < (len(parray)); i++ {
		parray[i] = <-in[i]
		if parray[i].Kind == ip.EOF {
			nActive--
		} else {
			key := parray[i].Data[0:keylen]
			if key < lowestKey {
				lowestKey = key
				lowestPort = i
				lowestKeyFields = sliceKey(lowestKey, fieldLengths)
			}
		}
	}

	prevKeyFields := lowestKeyFields
	nClosesNeeded := nFields
	firstTime := true

	// Collation - search for lowest
	for nActive > 0 {
		lowestKey := highestKey
		lowestPort := 0
		for i := 0; i < nPorts; i++ {
			if parray[i].Kind != ip.EOF {
				key := parray[i].Data[0:keylen]
				if key < lowestKey {
					lowestKey = key
					lowestPort = i
					lowestKeyFields = sliceKey(lowestKey, fieldLengths)
				}
			}
		}
		if firstTime {
			firstTime = false
		} else {
			nClosesNeeded = sendBrackets(prevKeyFields, lowestKeyFields, out)
			prevKeyFields = lowestKeyFields
		}
		// send found record
		out <- parray[lowestPort]
		// fetch next
		nextrec := <-in[lowestPort]
		parray[lowestPort] = nextrec
		if nextrec.Kind == ip.EOF {
			nActive--
		}
	}

	// finished, close off
	for ; nClosesNeeded >= 0; nClosesNeeded-- {
		out <- ip.IP{Kind: ip.Close}
	}
	out <- ip.IP{Kind: ip.EOF}
}

func makeFieldArray(s string) ([]int, int) {
	re := regexp.MustCompile("([0-9]+),?")
	sa := re.FindAllString(s, -1)
	a := make([]int, len(sa))
	for i := 0; i < len(a); i++ {
		n := strings.Replace(sa[i], ",", "", -1)
		a[i], _ = strconv.Atoi(n)
	}
	return a, len(a)
}

func makeKey(keylen int, value byte) string { // unicode left as exercise
	str := make([]byte, keylen)
	for i := 0; i < len(str); i++ {
		str[i] = value
	}
	return string(str)
}

func sliceKey(key string, fieldArray []int) []string {
	// to make level comparison simpler, chop up the key into
	// its component fields, returning a slice of fields
	result := make([]string, len(fieldArray))
	index := 0
	for i := 0; i < len(fieldArray); i++ {
		flen := fieldArray[i]
		result[i] = key[index : index+flen]
		index += flen
	}
	return result
}

func sendBrackets(prev, curr []string, out chan<- ip.IP) int {
	// [111 AA 11111] vs [111 AA 22222]
	// [111 AA 11111] vs [111 BB 11111]
	depth := 0
	for depth < len(curr) && prev[depth] == curr[depth] {
		depth += 1
	}
	nbrack := len(prev) - 1 - depth // calculate depth - index where mismatch begins
	for j := nbrack; j >= 0; j-- {
		out <- ip.IP{Kind: ip.Close}
	}
	for j := nbrack; j >= 0; j-- {
		out <- ip.IP{Kind: ip.Open}
	}
	return depth
}
