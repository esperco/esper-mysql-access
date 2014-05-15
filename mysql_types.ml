open Printf

module type Serializable =
sig
  type t
  val to_string : t -> string
  val of_string : string -> t
end

module type Numeric =
sig
  type t
  val to_float : t -> float
  val of_float : float -> t
end

type direction = [ `Asc | `Desc ]
    (* ascending or descending order *)

(* used to write float data into the 'ord' columns *)
let ord_of_float x =
  match classify_float x with
    FP_normal
  | FP_subnormal
  | FP_zero -> sprintf "%.17g" x (* enough digits to not lose precision *)
  | FP_infinite
  | FP_nan -> invalid_arg (sprintf "ord_of_float: %g" x)

(* Commonly-used created/last-modified timestamps *)
module KV = struct
  let created k v = Util_time.now ()
  let last_modified = Some (fun k v ord -> Util_time.now ())
end

module KKV = struct
  let created k1 k2 v = Util_time.now ()
  let last_modified = Some (fun k1 k2 v ord -> Util_time.now ())
end

module K2V = struct
  let created k1 k2 v = Util_time.now ()
end
