# Twunproxy
A Redis proxy client for enabling operations unsupported by [Twemproxy](https://github.com/twitter/twemproxy).

### Prerequisites

#### Go
Ensure you have a [Go](http://golang.org/doc/install) environment set up.

#### GoMock, MockGen
[GoMock](http://godoc.org/code.google.com/p/gomock/gomock) is required for testing. MockGen is required if you need to (re)generate mocks for interfaces.
```
go get github.com/golang/mock/gomock
go get github.com/golang/mock/mockgen
```


