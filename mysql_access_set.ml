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

module type Set_param =
sig
  val tblname : string
  module Key1 : Serializable
  module Key2 : Serializable
  module Ord : Numeric
end

type direction = Asc | Desc
    (* ascending or descending order *)

module type Set =
sig
  val tblname : string
  type key1
  type key2
  type ord

  (* Operations on multiple elements *)

  val get_all : key1 -> (key2 * ord) list Lwt.t
    (* return all elements of a set in no particular order *)

  val get_page_by_key :
    key1 -> int -> direction -> key2 option -> (key2 * ord) list Lwt.t

  val get_page_by_ord :
    key1 -> int -> direction -> (ord * key2) option -> (key2 * ord) list Lwt.t

  (* Operations on a single element *)

  val get_ord : key1 -> key2 -> ord option Lwt.t
  val exists : key1 -> key2 -> bool Lwt.t
  val add : key1 -> key2 -> ord -> unit Lwt.t
  val remove : key1 -> key2 -> unit Lwt.t

  (**/**)
  (* testing only; not the authoritative copy for this schema *)
  val create_table : unit -> unit Lwt.t
end

module Make (Param : Set_param) : Set
  with type key1 = Param.Key1.t
  and type key2 = Param.Key2.t
  and type ord = Param.Ord.t =

(*** Implementation ***)

struct
  open Printf
  open Lwt

  type key1 = Param.Key1.t
  type key2 = Param.Key2.t
  type ord = Param.Ord.t
  let tblname = Param.tblname
  let esc_tblname = Mysql.escape tblname
  let esc_key1 key = Mysql.escape (Param.Key1.to_string key)
  let esc_key2 key = Mysql.escape (Param.Key2.to_string key)
  let esc_ord ord =
    let x = Param.Ord.to_float ord in
    match classify_float x with
        FP_normal
      | FP_subnormal
      | FP_zero -> sprintf "%.17g" x
      | FP_infinite
      | FP_nan -> invalid_arg (sprintf "esc_ord: %g" x)

  let get_all k1 =
    let st =
      sprintf "select k2, ord from %s where k1='%s';"
        esc_tblname (esc_key1 k1)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let res = Mysql_lwt.unwrap_result x in
      let rows = Mysql_util.fetch_all res in
      BatList.map (function
        | [| Some k2; Some ord |] ->
            (Param.Key2.of_string k2, Param.Ord.of_float (float_of_string ord))
        |  _ -> failwith ("Broken result returned on: " ^ st)
      ) rows
    )

  let get_page_by_key key length direction opt_start =
    failwith "get_page_by_key: not implemented"

  let get_page_by_ord key length direction opt_start =
    failwith "get_page_by_key: not implemented"

  let get_ord k1 k2 =
    let st =
      sprintf "select ord from %s where k1='%s' and k2='%s';"
        esc_tblname (esc_key1 k1) (esc_key2 k2)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let res = Mysql_lwt.unwrap_result x in
      match Mysql.fetch res with
        | Some [| Some ord |] -> Some (Param.Ord.of_float (float_of_string ord))
        | None -> None
        |  _ -> failwith ("Broken result returned on: " ^ st)
    )

  let exists k1 k2 =
    get_ord k1 k2 >>= function
      | None -> return false
      | Some _ -> return true

  let add k1 k2 ord =
    let st =
      sprintf "\
replace into %s (k1, k2, ord) values ('%s', '%s', %s);
"
        esc_tblname (esc_key1 k1) (esc_key2 k2) (esc_ord ord)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res = Mysql_lwt.unwrap_result x in
      ()
    )

  let remove k1 k2 =
    let st =
      sprintf "delete from %s where k1='%s' and k2='%s';"
        esc_tblname (esc_key1 k1) (esc_key2 k2)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res = Mysql_lwt.unwrap_result x in
      ()
    )

  (* NOT the authoritative copy of the schema. Do not use in production. *)
  let create_table () =
    let st =
      sprintf "\
create table if not exists %s (
       k1 varchar(767) character set ascii not null,
         -- identifies the set
       k2 varchar(767) character set ascii not null,
         -- identifies an element of the set
         -- (unique within the set but not within the table)
       ord double not null,
         -- ordering, typically a timestamp

       primary key (k1, k2),
       index (k1),
       index (k2),
       index (ord)
) engine=InnoDB;
"
        esc_tblname
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res = Mysql_lwt.unwrap_result x in
      ()
    )
end


let test () =
  let module Param = struct
    let tblname = "testset"
    module Int_key = struct
      type t = int
      let to_string = string_of_int
      let of_string = int_of_string
    end
    module Timestamp_ord = struct
      type t = Util_time.t
      let to_float = Util_time.to_float
      let of_float = Util_time.of_float
    end
    module Key1 = Int_key
    module Key2 = Int_key
    module Ord = Timestamp_ord
  end
  in
  let module Testset = Make (Param) in
  let open Lwt in
  Lwt_main.run (
    Testset.create_table () >>= fun () ->
    let k1 = 1 in
    let l1 = [
      11, (Util_time.of_float 1.);
      12, (Util_time.of_float 2.);
      13, (Util_time.of_float 3.);
      14, (Util_time.of_float 4.);
    ] in
    Lwt_list.iter_s (fun (k2, ord) ->
      Testset.add k1 k2 ord
    ) l1 >>= fun () ->
    Testset.get_all k1 >>= fun l1' ->
    assert (List.sort compare l1' = List.sort compare l1);
    return true
  )

let tests = [
  "basic list operations", test;
]
