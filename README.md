# Native Go Zookeeper Client Library

[![zk](https://github.com/QuangTung97/zk/actions/workflows/go.yml/badge.svg)](https://github.com/QuangTung97/zk/actions/workflows/go.yml)
[![Coverage Status](https://coveralls.io/repos/github/QuangTung97/zk/badge.svg?branch=master)](https://coveralls.io/github/QuangTung97/zk?branch=master)

Forked from https://github.com/go-zookeeper/zk

But have replaced most of its interface to help achieve better guarantees:
* Watches, Response Function Calls and other Callbacks (Session Expired, Connection Retry, etc.)
are all happened in a single thread
* Strong watch guarantee: Watch Response Callback MUST happen BEFORE the Response of Create/Set/Delete
that affects the Watch
* When connection is disconnected, all pending operations MUST be finished or response with
errors before a new connection is going to be established

## Examples
Examples can be found here: https://github.com/QuangTung97/zk/tree/master/test-examples


## License

3-clause BSD. See LICENSE file.
