#!/usr/bin/env tarantool

box.cfg{
    background=true,
    username='tarantool',
    custom_proc_title='locksmith',
    slab_alloc_arena=0.1,
    work_dir='/var/lib/tarantool/locksmith/',
    pid_file = '/var/run/tarantool/locksmith.pid',
    logger = '/var/log/tarantool/locksmith.log',
    log_level=5,  -- info
    listen=11012,
}

box.schema.user.grant('guest', 'read,write,execute', 'universe', nil, {if_not_exists = true})

-- Import locksmith
dofile('/usr/local/tarantool-locksmith/locksmith.lua')
