start_server {tags {"limits"} overrides {maxclients 10}} {
    test {Check if maxclients works refusing connections} {
        set c 0
		set correctFailPoint 0
        catch {
            while {$c < 50} {
                incr c
				set correctFailPoint 1
                set rd [redis_deferring_client]
				set correctFailPoint 0
                $rd ping
                $rd read
                after 100
            }
        }
        assert {$c > 8 && $c <= 10}
        set correctFailPoint
    } 1
}
