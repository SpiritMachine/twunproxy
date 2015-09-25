package twunproxy

import (
	"errors"
	"time"
)

/******************************************************
 * Commands und utilities unsupported by Twemproxy.
 * New implementations will be added here.
 ******************************************************/

// BLPop implements the BLPOP Redis functionality that is unavailable using regular Twemproxy.
// NOTE: This version is only inplemented for a single key. Implementation of the full command is pending.
func (r *ProxyConn) BLPop(key string, timeout time.Duration) (string, error) {

	// If the command times out, it will not return a slice of results and is therefore not accepted
	canMap := func(v interface{}) bool {
		_, ok := v.([]interface{})
		return ok
	}

	cmd := RedisCmd{
		name: "BLPOP",
		key:  key,
		args: []interface{}{timeout.Seconds()},
	}

	v, err := r.Do(&cmd, canMap)
	if err != nil {
		return "", err
	}

	// This check is required for the case where the key has been mapped, but we still get a timeout.
	if r, ok := v.([]interface{}); ok {
		return string(r[1].([]byte)), nil
	}

	return "", errors.New("BLPOP timed out.")
}

// BGSave runs a background save on each instance, sleeping for the input duration between each save.
// The number of successfully issued BGSAVE commands is returned.
// This is usefull to ensure that multiple large Redis instances don't fork at once to persist to disk.
// Remember to disable persistence in configuration when using this feature.
func (r *ProxyConn) BGSave(interval time.Duration) (int, error) {
	i := 0

	for _, pool := range r.Pools {
		c := pool.Get()
		defer c.Close()

		if _, err := c.Do("BGSAVE"); err != nil {
			return i, err
		}

		i++
		time.Sleep(interval)
	}

	return i, nil
}
