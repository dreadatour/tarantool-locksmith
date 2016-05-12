-------------------------------------------------------------------------------
-- Locksmith: locks implementation for Tarantool
-------------------------------------------------------------------------------
local fiber = require('fiber')
local uuid = require('uuid')
local log = require('log')


-- lock space schema
local l_id          = 1   -- lock id
local l_name        = 2   -- lock name
local l_uid         = 3   -- lock unique identifier
local l_locked_at   = 4   -- lock creation time
local l_validity    = 5   -- lock validity time

-- consumer space schema
local c_session_id   = 1  -- session id
local c_fiber_id     = 2  -- fiber id
local c_lock_id      = 3  -- lock id
local c_connected_at = 4  -- consumer connection time


-- Statistics methods.
local statistics = {
    inc = function(self, metric)
        self[metric] = self[metric] + 1
        return self[metric]
    end,
    dec = function(self, metric)
        self[metric] = self[metric] - 1
        return self[metric]
    end
}
setmetatable(statistics, {
    __index = function(self, metric)
        rawset(self, metric, 0)
        return 0
    end
})

-- Convert seconds to microseconds.
function sec2usec(seconds)
    return 0ULL + seconds * 1000000
end

-- Convert microseconds to seconds.
function usec2sec(microseconds)
    return tonumber(microseconds) / 1000000
end


locksmith = {}
local method = {}


-- Create lock.
function method._lock_create(self, lock_name, validity)
    local now = fiber.time()
    local lock_uid = uuid.str()

    local max_id = self._space.index.lock_id:max()
    local lock_id = max_id ~= nil and max_id[l_id] + 1 or 0

    local lock = self._space:insert{
        lock_id,
        lock_name,
        lock_uid,
        sec2usec(now),
        sec2usec(now + validity)
    }
    if self._fiber ~= nil and self._fiber:status() ~= 'dead' then
        self._fiber:wakeup()
    end

    statistics:inc('lock_create')
    log.info("<Lock uid='" .. lock_uid .. "' name='" .. lock_name .. "'> created")

    return lock
end


-- Update lock.
function method._lock_update(self, lock_id, validity)
    local lock = self._space:update(lock_id, {
        {'=', 5, sec2usec(fiber.time() + validity)},
    })
    if self._fiber ~= nil and self._fiber:status() ~= 'dead' then
        self._fiber:wakeup()
    end

    statistics:inc('lock_update')
    log.info("<Lock uid='" .. lock[l_uid] .. "' name='" .. lock[l_name] .. "'> validity updated for " .. validity .. " sec")

    return lock
end


-- Delete lock
function method._lock_delete(self, lock_id)
    local lock = self._space:delete(lock_id)

    statistics:inc('lock_delete')
    log.info("<Lock uid='" .. lock[l_uid] .. "' name='" .. lock[l_name] .. "'> deleted")

    local consumer = self._queue.index.wait_for_lock:min{lock[l_id]}
    if consumer ~= nil then
        local consumer_fiber = fiber.find(consumer[c_fiber_id])
        if consumer_fiber ~= nil and consumer_fiber:status() ~= 'dead' then
            consumer_fiber:wakeup()
        end
    end

    if self._fiber ~= nil and self._fiber:status() ~= 'dead' then
        self._fiber:wakeup()
    end

    return lock
end


-- Acquire lock
function method.acquire(self, lock_name, validity, timeout)
    if lock_name == nil then
        box.error(box.error.PROC_LUA, "Lock name is not defined")
    end

    if validity == nil then
        box.error(box.error.PROC_LUA, "Lock validity is not defined")
    end

    if timeout == nil then
        timeout = 315360000  -- 10 * 365 * 86400 = 10 years (must be enough :)
        log.info("Try to acquire lock '" .. lock_name .. "' for " .. validity .. " sec, wait forever")
    elseif timeout == 0 then
        log.info("Try to acquire lock '" .. lock_name .. "' for " .. validity .. " sec, do not wait")
    else
        log.info("Try to acquire lock '" .. lock_name .. "' for " .. validity .. " sec, wait for " .. timeout .. " sec")
    end

    statistics:inc('acquire')

    -- if resource is not locked, create lock and exit
    local lock = self._space.index.name:get{lock_name}
    if lock == nil then
        statistics:inc('acquire_success')
        return self:_lock_create(lock_name, validity)
    end

    -- if resource is locked with another session, wait for `timeout` seconds until resource will be free
    if timeout ~= nil then
        local started_at
        local lock_id = lock[l_id]

        while timeout > 0 do
            started_at = sec2usec(fiber.time())

            -- create fiber and fall to sleep
            self._queue:insert{box.session.id(), fiber.id(), lock_id, started_at}
            fiber.sleep(timeout)
            self._queue:delete{box.session.id(), fiber.id()}

            -- try to acquire lock on wakeup
            lock = self._space.index.name:get{lock_name}
            if lock == nil then
                statistics:inc('acquire_success')
                return self:_lock_create(lock_name, validity)
            end

            timeout = timeout - (fiber.time() - usec2sec(started_at))
        end
    end

    -- report about lock acquire failed
    statistics:inc('acquire_fail')
    log.info("Lock '" .. lock_name .. "' is not acquired")
    return nil
end


-- Update lock validity.
function method.update(self, uid, validity)
    if uid == nil then
        box.error(box.error.PROC_LUA, "Lock uid is not defined")
    end

    if validity == nil then
        box.error(box.error.PROC_LUA, "Lock validity is not defined")
    end

    statistics:inc('update')
    log.info("Try to update <Lock uid='" .. uid .. "'> validity for " .. validity .. " sec")

    local lock = self._space.index.uid:get{uid}
    if lock == nil then
        statistics:inc('update_fail')
        log.info("Can't update <Lock uid='" .. uid .. "'>: lock is not set")
        return nil
    end

    lock = self:_lock_update(lock[l_id], validity)

    statistics:inc('update_success')
    log.info("<Lock uid='" .. uid .. "'> validity updated")

    return lock
end


-- Release lock if acquired.
function method.release(self, uid)
    if uid == nil then
        box.error(box.error.PROC_LUA, "Lock uid is not defined")
    end

    statistics:inc('release')
    log.info("Try to release <Lock uid='" .. uid .. "'>")

    local lock = self._space.index.uid:get{uid}
    if lock == nil then
        statistics:inc('release_fail')
        log.info("Can't release <Lock uid='" .. uid .. "'>: lock is not set")
        return nil
    end

    lock = self:_lock_delete(lock[l_id])

    statistics:inc('release_success')
    log.info("<Lock uid='" .. uid .. "'> released")

    return lock
end


-- Return locks statistics.
function method.statistics(self)
    local result = {
        calls = {},
        locks = {},
        consumers = {},
    }

    for name, value in pairs(statistics) do
        if type(value) ~= 'function' then
            result['calls'][tostring(name)] = value
        end
    end

    result.locks['count'] = self._space.index.lock_id:count()
    result.consumers['waiting'] = self._queue.index.pk:count()

    return result
end


-- Locks watchdog fiber.
function method._watchdog(self)
    log.info("Start lock watchdog fiber")

    fiber.name('locks_watchdog')

    local sleep_time, now, lock

    while true do
        sleep_time = 60  -- wake up every 60 seconds (just in case)
        now = sec2usec(fiber.time())

        -- check locks for expiration
        lock = self._space.index.release_at:min{}
        if lock ~= nil then
            if now >= lock[l_validity] then
                lock = self:_lock_delete(lock[l_id])
                statistics:inc('watchdog_release')
                log.info("<Lock uid='" .. lock[l_uid] .. "' name='" .. lock[l_name] .. "'> expired and released by watchdog")
                sleep_time = 0
            else
                local _st = usec2sec(lock[l_validity] - now)
                if _st < sleep_time then
                    sleep_time = _st
                end
            end
        end

        if sleep_time > 0 then
            lock = nil  -- free refcounter
            fiber.sleep(sleep_time)
            log.info("Lock watchdog wakeup")
        end
    end
end


-- Locks session disconnect callback.
function method._on_session_disconnect()
    log.info("Session disconnected")

    local consumer, consumer_fiber

    -- wakeup all waiters
    while true do
        consumer = locksmith._queue.index.pk:min{box.session.id()}
        if consumer == nil then
            break
        end

        log.info("Delete consumer " .. consumer)

        locksmith._queue:delete{consumer[c_session_id], consumer[c_fiber_id]}

        consumer_fiber = fiber.find(consumer[c_fiber_id])
        if consumer_fiber ~= nil and consumer_fiber:status() ~= 'dead' then
            consumer_fiber:wakeup()
        end
    end
end


-- Create or join infrastructure.
function method.start(self)
    log.info("Create locks spaces and indexes if needed")

    self._space = box.space._locksmith
    if self._space == nil then
        self._space = box.schema.space.create('_locksmith')
        self._space:create_index('lock_id',  {type = 'tree', parts = {l_id, 'NUM'}})
        self._space:create_index('name', {type = 'tree', parts = {l_name, 'STR'}})
        self._space:create_index('uid', {type = 'tree', parts = {l_uid, 'STR'}})
        self._space:create_index('release_at', {type = 'tree', parts = {l_validity, 'NUM'}, unique = false})
        log.info("New space 'locksmith' created")
    end

    self._queue = box.space._locksmith_queue
    if self._queue == nil then
        self._queue = box.schema.create_space('_locksmith_queue', {temporary = true})
        self._queue:create_index('pk', {type = 'tree', parts = {c_session_id, 'num', c_fiber_id, 'num'}})
        self._queue:create_index('wait_for_lock', {type = 'tree', parts = {c_lock_id, 'num', c_connected_at, 'num'}, unique = false})
        log.info("New space 'locksmith_queue' created")
    end

    self._fiber = fiber.create(self._watchdog, self)
    box.session.on_disconnect(locksmith._on_session_disconnect)
end


-- Initialize locksmith.
function method.initialize()
    log.info("Initialize locksmith")

    -- create user if user is not exists
    box.schema.user.create('locksmith', {if_not_exists = true})
    -- grant access to this user if grants are not exists
    box.schema.user.grant('locksmith', 'read,write,execute', 'universe', nil, {if_not_exists = true})
    -- become a user
    box.session.su('locksmith')

    -- create spaces and indexes
    method:start()

    -- create functions with 'suid' flag if functions is not exists
    box.schema.func.create('locksmith:acquire', {if_not_exists = true, setuid = true})
    box.schema.func.create('locksmith:update', {if_not_exists = true, setuid = true})
    box.schema.func.create('locksmith:release', {if_not_exists = true, setuid = true})
    box.schema.func.create('locksmith:statistics', {if_not_exists = true, setuid = true})

    -- become admin
    box.session.su('admin')
    -- revoke grants if grants are exists
    box.schema.user.revoke('locksmith', 'execute,read,write', 'universe', nil, {if_exists = true})
end


setmetatable(locksmith, {__index = method})

locksmith.initialize()
