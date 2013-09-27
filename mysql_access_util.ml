(*
  General-purpose module that can be passed to the
  Mysql_access_*.Make functors.
*)

module Ignore = struct
  type t = unit
  let to_string () = ""
  let of_string s = ()
  let to_float () = 0.
  let of_float x = ()
end

module String = struct
  type t = string
  let of_string s = s
  let to_string s = s
end
