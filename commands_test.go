package twunproxy

import (
	"github.com/golang/mock/gomock"
	"testing"
	"time"
)

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
