start_server {tags {"maxmemory"}} {
    foreach policy {
        allkeys-random allkeys-lru volatile-lru volatile-random volatile-ttl
        
    } {
        test "maxmemory - is the memory limit honoured? (policy $policy) with protect" {
            # make sure to start with a blank instance
            r flushall 
            # Get the current memory limit and calculate a new limit.
            # We just add 100k to the current memory size so that it is
            # fast for us to reach that limit.
            set used [s used_memory]
            set limit [expr {$used+100*1024}]
            r config set maxmemory $limit
            r config set maxmemory-policy $policy
            # Now add keys until the limit is almost reached.
            set numkeys 0
            while 1 {
                set key [randomKey]
                if {[r set $key x ex 10000 nx] == "OK"}  {
                    incr numkeys 
                }
                if {$numkeys < 21 }  {
                    set protected($numkeys) $key
                    r protect $key 
                }
                if {[s used_memory]+4096 > $limit} {
                    assert {$numkeys > 10}
                    break
                }
            }
            # If we add the same number of keys already added again, we
            # should still be under the limit.
            for {set j 0} {$j < $numkeys} {} {
                if {[r set [randomKey] x ex 10000 nx] == "OK"}  {
                    incr j 
                }
            }
            for {set j 1} {$j < 21} {incr j} {
                assert {[r get $protected($j)] == "x" }
            }
            assert {[s used_memory] < ($limit+4096)}
        }
    }

    foreach policy {
        allkeys-random allkeys-lru volatile-lru volatile-random volatile-ttl
    } {
        test "maxmemory - only allkeys-* should remove non-volatile keys ($policy)" {
            # make sure to start with a blank instance
            r flushall 
            # Get the current memory limit and calculate a new limit.
            # We just add 100k to the current memory size so that it is
            # fast for us to reach that limit.
            set used [s used_memory]
            set limit [expr {$used+100*1024}]
            r config set maxmemory $limit
            r config set maxmemory-policy $policy
            # Now add keys until the limit is almost reached.
            set numkeys 0
            while 1 {
                set key [randomKey]
                if {[r set $key x nx] == "OK"}  {
                    incr numkeys 
                }
                if {$numkeys < 21 }  {
                    set protected($numkeys) $key
                    r protect $key 
                }
                if {[s used_memory]+4096 > $limit} {
                    assert {$numkeys > 10}
                    break
                }
            }
            # If we add the same number of keys already added again and
            # the policy is allkeys-* we should still be under the limit.
            # Otherwise we should see an error reported by Redis.
            set err 0
            for {set j 0} {$j < $numkeys} {} {
                if {[catch {
                    if {[r set [randomKey] x nx] == "OK"}  {
                        incr j 
                    }
                    } e]
                    } {
                    incr j
                    if {[string match {*used memory*} $e]} {
                        set err 1
                    }
                }
            }
            if {[string match allkeys-* $policy]} {
                assert {[s used_memory] < ($limit+4096)}
            } else {
                assert {$err == 1}
            }
            for {set j 1} {$j < 21} {incr j} {
                assert {[r get $protected($j)] == "x" }
            }
        }
    }

    foreach policy {
        volatile-lru volatile-random volatile-ttl
    } {
        test "maxmemory - policy $policy should only remove volatile keys. - protect" {
            # make sure to start with a blank instance
            r flushall 
            # Get the current memory limit and calculate a new limit.
            # We just add 100k to the current memory size so that it is
            # fast for us to reach that limit.
            set used [s used_memory]
            set limit [expr {$used+100*1024}]
            r config set maxmemory $limit
            r config set maxmemory-policy $policy
            # Now add keys until the limit is almost reached.
            set numkeys 0
            while 1 {
                # Odd keys are volatile
                # Even keys are non volatile
                if {$numkeys % 2} {
                    r setex "key:$numkeys" 10000 x
                } else {
                    r set "key:$numkeys" x
                }
                if {$numkeys < 20 }  {
                    r protect "key:$numkeys"
                }
                if {[s used_memory]+4096 > $limit} {
                    assert {$numkeys > 10}
                    break
                }
                incr numkeys
            }
            # Now we add the same number of volatile keys already added.
            # We expect Redis to evict only volatile keys in order to make
            # space.
            set err 0
            for {set j 0} {$j < $numkeys} {incr j} {
                catch {r setex "foo:$j" 10000 x}
            }
            # We should still be under the limit.
            assert {[s used_memory] < ($limit+4096)}
            # However all our non volatile keys should be here.
            for {set j 0} {$j < $numkeys} {incr j 2} {
                assert {[r exists "key:$j"]}
            }
            for {set j 0} {$j < 20} {incr j } {
                assert {[r exists "key:$j"]}
            }
        }
    }
}
