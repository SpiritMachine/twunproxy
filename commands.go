package twunproxy

import (
	"time"
)

/******************************************************
 * Commands und utilities unsupported by Twemproxy.
 * New implementations will be added here.
 ******************************************************/

// BGSave runs a background save on each instance, sleeping for the input duration between each save.
// The number of successfully issued BGSAVE commands is returned.
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

// BLPop implements the BLPOP Redis functionality that is unavailable using regular Twemproxy.
func (r *ProxyConn) BLPop(key string, timeout time.Duration) (string, error) {
	canMap := func(v interface{}) bool {
		return v != nil
	}

	cmd := RedisCmd{
		name: "BLPOP",
		key:  key,
		args: []interface{}{timeout.Seconds()},
	}

	v, err := r.Do(&cmd, canMap)
	return string(v.([]interface{})[1].([]byte)), err
}
