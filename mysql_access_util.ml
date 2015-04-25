(*
  General-purpose module that can be passed to the
  Mysql_access_*.Make functors.
*)

(* Empty JSON object allowing future optional fields
   without breaking compatibility *)
module Unit = struct
  type t = unit
  let to_string () = "{}"
  let of_string s = ()
end

module String = struct
  type t = string
  let of_string s = s
  let to_string s = s
end

