Tarantool locksmith module
==========================

An application-level library that provides distributed locks for [tarantool 1.6](http://tarantool.org).


Install
-------

1. Clone this git repository to `/usr/local/tarantool-locksmith/.
2. Add `init.lua` to `/etc/tarantool/instances.enabled/`.
3. Restart tarantool: `sudo service tarantool restart`.

*FIXME*: Add RPM/DEB packages and rewrite installation instructions.


Usage
-----

1. Acquire new lock: `locksmith:acquire(lock_name, validity, timeout)`.

	Arguments:
	- `lock_name` - name of the lock (string)
	- `validity` - lock lifetime, seconds (float)
	- `timeout` - lock wait timeout, seconds (float); 0 - do not wait, `nil` - wait forever

	Returns:
	- lock tuple if lock acquire is sucessful: `[lock_id, lock_name, lock_uid, creation_time, valid_until]`
	- `nil` if lock acquire failed

	Examples:

	```
	> locksmith:acquire('foo', 60, 0)
	---
	- [0, 'foo', '45961e7f-3b47-49b5-9fc2-985b1e534aec', 1463088131062999, 1463088191062999]
	...
	```

	```
	> locksmith:acquire('foo', 60, 0)
	---
	- null
	...
	```

2. Update lock validity: `locksmith:update(lock_uid, validity)`.

	Arguments:
	- `lock_uid` - uid of the lock (string)
	- `validity` - new lock lifetime, seconds (float)

	Returns:
	- lock tuple if lock update is sucessful: `[lock_id, lock_name, lock_uid, creation_time, valid_until]`
	- `nil` if lock is missed or expired

	Examples:

	```
	> locksmith:update('45961e7f-3b47-49b5-9fc2-985b1e534aec', 60)
	---
	- [0, 'foo', '45961e7f-3b47-49b5-9fc2-985b1e534aec', 1463088131062999, 1463088206428052]
	...
	```

	```
	> locksmith:update('9c5c5354-f975-4811-aa7e-c6e569d359eb', 60)
	---
	- null
	...
	```

3. Release lock: `locksmith:release(lock_uid)`.

	Arguments:
	- `lock_uid` - uid of the lock (string)

	Returns:
	- lock tuple if lock release is sucessful: `[lock_id, lock_name, lock_uid, creation_time, valid_until]`
	- `nil` if lock is missed or expired

	Examples:

	```
	> locksmith:release('45961e7f-3b47-49b5-9fc2-985b1e534aec')
	---
	- [0, 'foo', '45961e7f-3b47-49b5-9fc2-985b1e534aec', 1463088131062999, 1463088206428052]
	...
	```

	```
	> locksmith:release('9c5c5354-f975-4811-aa7e-c6e569d359eb')
	---
	- null
	...
	```

4. Statistics: `locksmith:statistics()`.

	Example:
	```
	> locksmith:statistics()
	---
	- calls:
	    acquire: 1
	    acquire_success: 1
	    update: 1
	    update_success: 1
	    release: 1
	    release_success: 1
	    watchdog_release: 0
	    lock_create: 1
	    lock_update: 1
	    lock_delete: 1
	  locks:
	    count: 0
	  consumers:
	    waiting: 0
	...
	```
