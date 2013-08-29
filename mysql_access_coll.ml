module type Serializable =
sig
  type t
  val to_string : t -> string
  val of_string : string -> t
end

module type Order =
sig
  type t
  val to_float : t -> float
  val of_float : float -> t
end

module type Collection_param =
sig
  val tblname : string
  module Key1 : Serializable
  module Key2 : Serializable
  module Ord : Order
  type ord
end

(* not implemented *)
