package twunproxy

import (
	"github.com/golang/mock/gomock"
	"sync"
	"testing"
	"time"
)

func TestDoInstanceReturnsOnStopChannelMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn, mockPool := setupMockPool(ctrl)
	mockConn.EXPECT().Do("CMD", "KEY", "A1", "A2").Return(nil, nil)
	mockConn.EXPECT().Close().AnyTimes()

	proxy := getMockProxy(mockPool)

	results := make(chan RedisReturn)
	stop := make(chan bool)
	wg := new(sync.WaitGroup)
	wg.Add(1)

	gotReturn := false
	canMap := func(v interface{}) bool {
		time.Sleep(5 * time.Second)
		gotReturn = true
		return false
	}

	go proxy.doInstance(0, getRedisCmd(), canMap, results, stop, wg)
	time.Sleep(1 * time.Second)
	stop <- true
	wg.Wait()

	if gotReturn {
		t.Fatal("Expected return from Goroutine before Redis command return.")
	}
}

func TestDoInstanceReturnsOnBadCommandResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn, mockPool := setupMockPool(ctrl)
	mockConn.EXPECT().Do("CMD", "KEY", "A1", "A2").Return(true, nil)
	mockConn.EXPECT().Close()

	proxy := getMockProxy(mockPool)

	results := make(chan RedisReturn)
	stop := make(chan bool)
	wg := new(sync.WaitGroup)
	wg.Add(1)

	canMap := func(v interface{}) bool { return false }
	go proxy.doInstance(0, getRedisCmd(), canMap, results, stop, wg)

	var res RedisReturn
	go func() {
		for results != nil {
			select {
			case res = <-results:
				results = nil
			}
		}
	}()
	wg.Wait()

	if res.val != nil {
		t.Fatal("Unexpected Redis return value.")
	}

	if _, ok := proxy.KeyInstance["KEY"]; ok {
		t.Fatal("Got unexpected mapping entry for Redis key.")
	}
}

func TestDoInstanceWritesToChannelAndReturnsOnAcceptedResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn, mockPool := setupMockPool(ctrl)
	mockConn.EXPECT().Do("CMD", "KEY", "A1", "A2").Return(true, nil)
	mockConn.EXPECT().Close()

	proxy := getMockProxy(mockPool)

	results := make(chan RedisReturn)
	stop := make(chan bool)
	wg := new(sync.WaitGroup)
	wg.Add(1)

	canMap := func(v interface{}) bool { return true }
	go proxy.doInstance(0, getRedisCmd(), canMap, results, stop, wg)

	var res RedisReturn
	go func() {
		for results != nil {
			select {
			case res = <-results:
				stop <- true
				results = nil
			}
		}
	}()
	wg.Wait()

	if !res.val.(bool) {
		t.Fatal("Unexpected Redis return value.")
	}

	if _, ok := proxy.KeyInstance["KEY"]; !ok {
		t.Fatal("Expected mapping entry for Redis key.")
	}
}

func TestDoExecutesCommandOnAllProxyPools(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn1, mockPool1 := setupMockPool(ctrl)
	mockConn2, mockPool2 := setupMockPool(ctrl)
	mockConn3, mockPool3 := setupMockPool(ctrl)

	mockConn1.EXPECT().Do("CMD", "KEY", "A1", "A2").Return(nil, nil)
	mockConn1.EXPECT().Close()
	mockConn2.EXPECT().Do("CMD", "KEY", "A1", "A2").Return(nil, nil)
	mockConn2.EXPECT().Close()
	mockConn3.EXPECT().Do("CMD", "KEY", "A1", "A2").Return(interface{}(true), nil)
	mockConn3.EXPECT().Close()

	proxy := getMockProxy(mockPool1, mockPool2, mockPool3)

	resp, err := proxy.do(getRedisCmd(), func(v interface{}) bool { return v != nil })

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if resp.(bool) != true {
		t.Fatalf("Incorrect Response.")
	}
}

func TestSingleConnectionNonExistentKeyBLPOP(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	key := "parsed:soccer:league:event:match"
	response := "A correct response"

	mockConn, mockPool := setupMockPool(ctrl)
	mockConn.EXPECT().Do("BLPOP", key, 5.0).Return([]interface{}{[]byte(key), []byte(response)}, nil)
	mockConn.EXPECT().Close()

	proxy := getMockProxy(mockPool)

	if resp, err := proxy.BLPop(key, 5*time.Second); err != nil || resp != response {
		t.Fatalf("Did not receive expected command response.")
	}

	if _, ok := proxy.KeyInstance[key]; !ok {
		t.Fatal("Expected mapping entry for Redis key.")
	}
}

func TestMultipleConnectionNonExtantKeyBLPopReturnsCorrectlyAndAddsMapping(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	key := "parsed:soccer:league:event:match"
	response := "A correct response"

	mockConn1, mockPool1 := setupMockPool(ctrl)
	mockConn2, mockPool2 := setupMockPool(ctrl)
	mockConn1.EXPECT().Do("BLPOP", key, 5.0)
	mockConn1.EXPECT().Close()
	mockConn2.EXPECT().Do("BLPOP", key, 5.0).Return([]interface{}{[]byte(key), []byte(response)}, nil)
	mockConn2.EXPECT().Close()

	proxy := getMockProxy(mockPool1, mockPool2)

	if resp, err := proxy.BLPop(key, 5*time.Second); err != nil || resp != response {
		t.Fatalf("Did not receive expected command response.")
	}

	if _, ok := proxy.KeyInstance[key]; !ok {
		t.Fatal("Expected mapping entry for Redis key.")
	}
}

func TestSingleConnectionExtantKeyBLPopReturnsCorrectly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	key := "parsed:soccer:league:event:match"
	response := "A correct response"

	mockConn, mockPool := setupMockPool(ctrl)
	mockConn.EXPECT().Do("BLPOP", key, 5.0).Return([]interface{}{[]byte(key), []byte(response)}, nil)
	mockConn.EXPECT().Close()

	proxy := getMockProxy(mockPool)
	proxy.KeyInstance[key] = mockPool

	if resp, err := proxy.BLPop(key, 5*time.Second); err != nil || resp != response {
		t.Fatalf("Did not receive expected command response.")
	}
}

func TestMultipleConnectionExtantKeyBLPopReturnsCorrectly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	key := "parsed:soccer:league:event:match"
	response := "A correct response"

	mockConn1, mockPool1 := setupMockPool(ctrl)
	mockPool2 := NewMockConnGetter(ctrl)
	mockConn1.EXPECT().Do("BLPOP", key, 5.0).Return([]interface{}{[]byte(key), []byte(response)}, nil)
	mockConn1.EXPECT().Close()

	proxy := getMockProxy(mockPool1, mockPool2)
	proxy.KeyInstance[key] = mockPool1

	if resp, err := proxy.BLPop(key, 5*time.Second); err != nil || resp != response {
		t.Fatalf("Did not receive expected command response.")
	}
}

func TestBGSaveExecutesAgainstEachPool(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn1, mockPool1 := setupMockPool(ctrl)
	mockConn2, mockPool2 := setupMockPool(ctrl)
	mockConn1.EXPECT().Do("BGSAVE").Return(interface{}("+OK\r\n"), nil)
	mockConn1.EXPECT().Close()
	mockConn2.EXPECT().Do("BGSAVE").Return(interface{}("+OK\r\n"), nil)
	mockConn2.EXPECT().Close()

	n, err := getMockProxy(mockPool1, mockPool2).BGSave(1)

	if err != nil {
		t.Fatalf("BGSave Faild with error: %v", err)
	}

	if n != 2 {
		t.Fatalf("Incorrect Number of BGSave(s)")
	}
}

/******************************************************
 * Helpers
 ******************************************************/

func setupMockPool(ctrl *gomock.Controller) (*MockConn, ConnGetter) {
	mockConn := NewMockConn(ctrl)
	mockPool := NewMockConnGetter(ctrl)

	// Pool always returns the wrapped mock connection.
	mockPool.EXPECT().Get().AnyTimes().Return(mockConn)

	return mockConn, mockPool
}

func getMockProxy(pools ...ConnGetter) *ProxyConn {
	return &ProxyConn{
		Pools:       pools,
		KeyInstance: make(map[string]ConnGetter),
	}
}

func getRedisCmd() *RedisCmd {
	return &RedisCmd{
		name: "CMD",
		key:  "KEY",
		args: []interface{}{"A1", "A2"},
	}
}
