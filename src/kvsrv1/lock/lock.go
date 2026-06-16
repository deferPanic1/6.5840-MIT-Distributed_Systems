package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk

	lockname string
	lockid   string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck:       ck,
		lockname: lockname,
		lockid:   kvtest.RandValue(8),
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.lockname)

		switch err {
		case rpc.ErrNoKey:
			putErr := lk.ck.Put(lk.lockname, lk.lockid, 0)
			if putErr == rpc.OK {
				return
			}

			if putErr == rpc.ErrMaybe {
				if lk.checkOwned() {
					return
				}
			}

		case rpc.OK:
			if val == lk.lockid {
				return
			}

			if val == "" {
				putErr := lk.ck.Put(lk.lockname, lk.lockid, ver)

				if putErr == rpc.OK {
					return
				}

				if putErr == rpc.ErrMaybe {
					if lk.checkOwned() {
						return
					}
				}
			}

		}

		time.Sleep(20 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		val, ver, err := lk.ck.Get(lk.lockname)

		if err != rpc.OK || val != lk.lockid {
			return
		}

		putErr := lk.ck.Put(lk.lockname, "", ver)
		if putErr == rpc.OK {
			return
		}
		if putErr == rpc.ErrMaybe {
			v, _, e := lk.ck.Get(lk.lockname)
			if e == rpc.OK && v != lk.lockid {
				return
			}
			continue
		}
	}
}

func (lk *Lock) checkOwned() bool {
	val, _, err := lk.ck.Get(lk.lockname)
	return err == rpc.OK && val == lk.lockid
}
