# sp_net externs for the real transport. Top-level module: spinel's name
# resolver wants FFI plumbing outside nested modules (tep precedent).
# Distinctly named so it can coexist with other packages' sp_net modules
# in one program (duplicate extern declarations are fine).
#
# sp_net_recv_some blocks with scheduler-aware parking (sp_net_wait_io ->
# sp_sched_wait_io under SP_THREADS), so a blocking-style client is
# already a good citizen under the fiber scheduler.
module RedisSock
  ffi_func :sp_net_connect,     [:str, :int],       :int
  ffi_func :sp_net_close,       [:int],             :int
  ffi_func :sp_net_write_bytes, [:int, :str, :int], :int
  ffi_func :sp_net_recv_some,   [:int, :int],       :binstr
  # recv_some with a deadline in seconds (negative: none), and whether
  # the last timed read gave up on it (Spinel's sp_net, matz/spinel#9584).
  ffi_func :sp_net_recv_some_timeout, [:int, :int, :double], :binstr
  ffi_func :sp_net_take_recv_timeout, [],                    :int
  # connect with a deadline in seconds (negative: none), and whether the
  # last timed connect gave up on it rather than being refused.
  ffi_func :sp_net_connect_timeout,      [:str, :int, :double], :int
  ffi_func :sp_net_take_connect_timeout, [],                    :int
end
