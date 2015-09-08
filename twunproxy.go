package twunproxy

import (
	"errors"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"sync"
	"time"
)

// Conn interface represents the minimum implemented signature for underlying Redis connections.
type Conn interface {
	Close() error
	Do(commandName string, args ...interface{}) (reply interface{}, err error)
}

// ConnGetter is the interface that underlying Redis connection pools should implement.
type ConnGetter interface {
	Get() Conn
}

// RedisPoolConfig represents one named pool from a Twemproxy configuration file.
type RedisPoolConfig struct {
	Servers []string `yaml:"servers"`
	Auth    string   `yaml:"redis_auth"`
}

// RedisReturn allows us to pass Redis command returns around as a single value.
type RedisReturn struct {
	val interface{}
	err error
}

// RedisCmd is a container for all the requisite properties of a Redis command.
// Assumed usage is for commands where the key is the first argument after the command name.
type RedisCmd struct {
	name string
	key  string
	args []interface{}
}

// The 'Do' command accepts a variadic list of args after the command name.
// We need to create a single slice.
func (c *RedisCmd) getArgs() []interface{} {
	return append([]interface{}{c.key}, c.args...)
}

// ProxyConn maintains its own slice of Redis connection pools and mappings of Redis keys to pools.
type ProxyConn struct {
	Pools       []ConnGetter
	KeyInstance map[string]ConnGetter
}

// CreatePool is the signature for return a connection pool based on the input Redis address and auth strings.
type CreatePool func(string, string) ConnGetter

// Read the Twemproxy configuration file from the input path.
// Instantiate a ProxyConn based on the input pool name.
// Initialise a key-to-pool mapping with the input initial capacity.
func NewProxyConn(confPath, poolName string, keyCap int, create CreatePool) (*ProxyConn, error) {
	f, err := ioutil.ReadFile(confPath)
	if err != nil {
		return nil, err
	}

	var m map[string]RedisPoolConfig
	if err := yaml.Unmarshal(f, &m); err != nil {
		return nil, err
	}

	conf := m[poolName]
	pools := make([]ConnGetter, len(conf.Servers))
	for i, def := range conf.Servers {
		pools[i] = create(def, conf.Auth)
	}

	proxy := new(ProxyConn)
	proxy.Pools = pools
	proxy.KeyInstance = make(map[string]ConnGetter, keyCap)
	return proxy, nil
}

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

	v, err := r.do(&cmd, canMap)
	return string(v.([]interface{})[1].([]byte)), err
}

// Run the input command against the cluster.
// If we already have a pool mapped to the command key, just run it there and return the result.
// Otherwise set up Goroutines running against each connection in the pool.
// The Goroutines will terminate upon the first successful Redis command return.
// NOTE: Blocking commands should be issued with a timeout or risk blocking permanently.
func (r *ProxyConn) do(cmd *RedisCmd, canMap func(interface{}) bool) (interface{}, error) {
	// If we have already determined the instance for this key, just run it.
	if pool, ok := r.KeyInstance[cmd.key]; ok {
		conn := pool.Get()
		defer conn.Close()
		return conn.Do(cmd.name, cmd.getArgs()...)
	}

	// Start the command on each of the pools and receive results on a channel.
	results := make(chan RedisReturn)
	stop := make(chan bool)
	wg := new(sync.WaitGroup)
	for i := range r.Pools {
		wg.Add(1)
		go r.doInstance(i, cmd, canMap, results, stop, wg)
	}

	// Wait for the first accepted Redis command result then send a message on the stop channel to other Goroutines.
	// Goroutines started above will detect this condition and complete.
	res := RedisReturn{val: nil, err: errors.New("No results returned that could determine a key mapping.")}
	go func() {
		for results != nil {
			select {
			case res = <-results:
				stop <- true
				results = nil
			}
		}
	}()

	// Wait for all the Redis connections to run their operations.
	wg.Wait()

	// Causes Goroutine above to complete in the event that no connection returned an accepted result.
	results = nil

	return res.val, res.err
}

// Runs the input Redis command against a connection from the input pool.
// If the canMap test returns true for the result, the key is mapped to the pool.
// The result is then sent on the result channel, which causes a subsequent message on the stop channel.
// Any Redis command return causes the wait group to be notified and a return from the method.
// The last remaining path is for the a message on the stop channel before a return is received from the Redis command.
// This causes wait group notification and return.
func (r *ProxyConn) doInstance(
	pIdx int,
	cmd *RedisCmd,
	canMap func(interface{}) bool,
	res chan RedisReturn,
	stop chan bool,
	wg *sync.WaitGroup) {

	defer wg.Done()
	pool := r.Pools[pIdx]

	// This is outside the Goroutine below to ensure connection closure.
	conn := pool.Get()
	defer conn.Close()

	// Start the command on a new Goroutine.
	// If we receive a return, test it and add a mapping if we have located the instance correctly.
	// If so, send the return on the channel if it still exists.
	cmdDone := make(chan bool)
	go func() {
		if val, err := conn.Do(cmd.name, cmd.getArgs()...); canMap(val) {
			// NOTE: A nil channel here means there was a return accepted from another Redis instance.
			// This indicates a bad canMap definition. This check prevents a panic.
			if res != nil {
				r.KeyInstance[cmd.key] = pool
				res <- RedisReturn{val: val, err: err}

				for res != nil {
					// If we sent a result, wait for the receiver to nullify the channel.
					// This prevents a race condition where the wait group can be notified before the channel is read.
				}
			}
		}

		cmdDone <- true
	}()

	// Wait for completion of this command or completion notification from any others.
	select {
	case <-stop:
		return
	case <-cmdDone:
		return
	}
}
