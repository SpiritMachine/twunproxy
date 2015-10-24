package twunproxy

import (
	"errors"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"sync"
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
type redisPoolConfig struct {
	Servers []string `yaml:"servers"`
	Auth    string   `yaml:"redis_auth"`
}

// RedisReturn allows us to pass Redis command returns around as a single value.
type redisReturn struct {
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

// CreatePool is the signature for returning a connection pool based on the input Redis address and auth strings.
type CreatePool func(string, string) ConnGetter

// NewProxyConn creates a proxy for all of the connections in a Twemproxy-fronted pool.
// Read the Twemproxy configuration file from the input path.
// Instantiate a ProxyConn based on the input pool name.
// Initialise a key-to-pool mapping with the input initial capacity.
func NewProxyConn(confPath, poolName string, keyCap int, create CreatePool) (*ProxyConn, error) {
	f, err := ioutil.ReadFile(confPath)
	if err != nil {
		return nil, err
	}

	var m map[string]redisPoolConfig
	if err := yaml.Unmarshal(f, &m); err != nil {
		return nil, err
	}

	conf := m[poolName]
	pools := make([]ConnGetter, len(conf.Servers))

	// For each instance described in the Twemproxy configuration, create a connection pool.
	// Execute a PING command to check that it is valid and available.
	for i, def := range conf.Servers {
		p := create(def, conf.Auth)

		c := p.Get()
		defer c.Close()
		if _, err := c.Do("PING"); err != nil {
			return nil, err
		}

		pools[i] = p
	}

	proxy := new(ProxyConn)
	proxy.Pools = pools
	proxy.KeyInstance = make(map[string]ConnGetter, keyCap)
	return proxy, nil
}

// Run the input command against the cluster.
// If we already have a pool mapped to the command key, just run it there and return the result.
// Otherwise set up Goroutines running against each connection in the pool.
// The Goroutines will terminate upon the first successful Redis command return.
// NOTE: Blocking commands should be issued with a timeout or risk blocking permanently.
func (r *ProxyConn) Do(cmd *RedisCmd, canMap func(interface{}) bool) (interface{}, error) {
	// If we have already determined the instance for this key, just run it.
	if pool, ok := r.KeyInstance[cmd.key]; ok {
		conn := pool.Get()
		defer conn.Close()
		return conn.Do(cmd.name, cmd.getArgs()...)
	}

	// Start the command on each of the pools and receive results on a channel.
	results := make(chan redisReturn)
	wg := new(sync.WaitGroup)
	stop := make([]chan bool, len(r.Pools))
	for i := range r.Pools {
		// Buffer prevents blocking when sending stop commands to completed Goroutines.
		stop[i] = make(chan bool, 1)
		wg.Add(1)
		go r.doInstance(i, cmd, canMap, results, stop[i], wg)
	}

	// Wait for the first accepted Redis command result then send a message on the stop channel to other Goroutines.
	// Goroutines started above will detect this condition and complete.
	res := redisReturn{val: nil, err: errors.New("No results returned that could determine a key mapping.")}
	go func() {
		for rr := range results {
			res = rr
			for _, c := range stop {
				c <- true
			}
		}
	}()

	// Wait for all the Redis connections to run their operations.
	wg.Wait()
	close(results)

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
	res chan redisReturn,
	stop chan bool,
	wg *sync.WaitGroup) {

	defer wg.Done()
	pool := r.Pools[pIdx]

	// This is outside the Goroutine below to ensure connection closure.
	conn := pool.Get()
	defer conn.Close()

	// Start the command on a new Goroutine.
	// If we receive a return, test it and add a mapping if we have located the instance correctly.
	// If we have, send the return on the results channel.

	// NOTE: Bad canMap definitions can result in panics here.
	// If the definition returns true for more than one result, there will be an attempted write to a closed channel.
	cmdDone := make(chan bool)
	go func() {
		if val, err := conn.Do(cmd.name, cmd.getArgs()...); canMap(val) {
			r.KeyInstance[cmd.key] = pool
			res <- redisReturn{val: val, err: err}
		} else {
			cmdDone <- true
		}
	}()

	// Wait for completion of this command or notification of accepted return from any others.
	select {
	case <-stop:
		return
	case <-cmdDone:
		return
	}
}
